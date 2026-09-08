#!/usr/bin/env python3
"""
ETL dos Dados Publicos de CNPJ (Receita Federal) filtrado por CNAE.

Baixa apenas os arquivos necessarios, filtra as linhas dos CNAEs desejados lendo
direto de dentro dos .zip (sem descompactar 15 GB em disco) e monta um documento
por estabelecimento, ja com empresa, socios e descricoes de dominio resolvidos.

A saida vai para NDJSON (um documento por linha, formato nativo do mongoimport)
e/ou direto para uma collection do MongoDB. A fase pesada roda inteira aqui, numa
instancia separada; o servidor de producao so recebe os documentos prontos.
"""

import datetime
import gzip
import hashlib
import os
import pathlib
import re
import sys
import time
import xml.etree.ElementTree as ET
import zipfile
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests
from bson import json_util
from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient
from requests.auth import HTTPBasicAuth

RFB_SHARE_TOKEN = 'YggdBLfdninEJX9'
RFB_WEBDAV_BASE = 'https://arquivos.receitafederal.gov.br/public.php/webdav'
RFB_AUTH = HTTPBasicAuth(RFB_SHARE_TOKEN, '')
DAV_NS = {'d': 'DAV:'}

COLS_ESTAB = [
    'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia',
    'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral',
    'nome_cidade_exterior', 'pais', 'data_inicio_atividade', 'cnae_fiscal_principal',
    'cnae_fiscal_secundaria', 'tipo_logradouro', 'logradouro', 'numero', 'complemento',
    'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2',
    'ddd_fax', 'fax', 'correio_eletronico', 'situacao_especial', 'data_situacao_especial',
]
CAMPOS_ESTAB = [
    'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia',
    'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral',
    'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria',
    'tipo_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf',
    'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'correio_eletronico',
]
SAIDA_ESTAB = ['cnpj'] + CAMPOS_ESTAB

COLS_EMPRESA = [
    'cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel',
    'capital_social', 'porte_empresa', 'ente_federativo_responsavel',
]
SAIDA_EMPRESA = [
    'cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel',
    'capital_social', 'porte_empresa',
]

COLS_SOCIOS = [
    'cnpj_basico', 'identificador_socio', 'nome_socio_razao_social', 'cpf_cnpj_socio',
    'qualificacao_socio', 'data_entrada_sociedade', 'pais', 'representante_legal',
    'nome_do_representante', 'qualificacao_representante_legal', 'faixa_etaria',
]
SAIDA_SOCIOS = [
    'cnpj_basico', 'identificador_socio', 'nome_socio_razao_social', 'cpf_cnpj_socio',
    'qualificacao_socio', 'data_entrada_sociedade', 'representante_legal',
    'nome_do_representante', 'qualificacao_representante_legal',
]

# Tabelas de dominio: zip remoto -> (tabela, sufixo do arquivo interno)
DOMINIOS = {
    'Cnaes.zip': ('cnae', 'CNAECSV'),
    'Municipios.zip': ('munic', 'MUNICCSV'),
    'Naturezas.zip': ('natju', 'NATJUCSV'),
    'Qualificacoes.zip': ('quals', 'QUALSCSV'),
    'Motivos.zip': ('moti', 'MOTICSV'),
    'Paises.zip': ('pais', 'PAISCSV'),
}


def log(msg):
    print(f'[{time.strftime("%H:%M:%S")}] {msg}', flush=True)


def makedirs(caminho):
    os.makedirs(caminho, exist_ok=True)


# --------------------------------------------------------------------------- #
# Descoberta e download                                                        #
# --------------------------------------------------------------------------- #
def webdav_list(path):
    resp = requests.request('PROPFIND', f'{RFB_WEBDAV_BASE}{path}', auth=RFB_AUTH,
                            headers={'Depth': '1'}, timeout=60)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)

    itens = []
    for response in root.findall('d:response', DAV_NS):
        href = response.find('d:href', DAV_NS).text
        nome = href.rstrip('/').split('/')[-1]
        if not nome or href.rstrip('/') == f'/public.php/webdav{path.rstrip("/")}':
            continue
        is_dir = response.find('.//d:resourcetype/d:collection', DAV_NS) is not None
        tam_el = response.find('.//d:getcontentlength', DAV_NS)
        tamanho = int(tam_el.text) if tam_el is not None and tam_el.text else 0
        itens.append({'name': nome, 'is_dir': is_dir, 'size': tamanho})
    return itens


def descobrir_mes(mes_fixo=None):
    if mes_fixo:
        return mes_fixo
    meses = sorted(i['name'] for i in webdav_list('/')
                   if i['is_dir'] and re.fullmatch(r'\d{4}-\d{2}', i['name']))
    if not meses:
        sys.exit('Nao foi possivel encontrar nenhuma pasta mensal no servidor da Receita Federal.')
    return meses[-1]


def baixar(nome, mes, destino, tamanho_esperado, tentativas=4):
    """Baixa um zip com retomada por Range e retorna o caminho local."""
    if os.path.isfile(destino) and tamanho_esperado and os.path.getsize(destino) == tamanho_esperado:
        log(f'ja baixado: {nome}')
        return destino

    url = f'{RFB_WEBDAV_BASE}/{mes}/{nome}'
    parcial = destino + '.parte'
    for tentativa in range(1, tentativas + 1):
        try:
            ja_baixado = os.path.getsize(parcial) if os.path.isfile(parcial) else 0
            headers = {'Range': f'bytes={ja_baixado}-'} if ja_baixado else {}
            inicio = time.time()
            with requests.get(url, auth=RFB_AUTH, stream=True, timeout=(30, 300),
                              headers=headers) as r:
                if ja_baixado and r.status_code != 206:
                    ja_baixado = 0  # servidor ignorou o Range
                r.raise_for_status()
                modo = 'ab' if ja_baixado else 'wb'
                with open(parcial, modo) as saida:
                    for pedaco in r.iter_content(chunk_size=4 * 1024 * 1024):
                        saida.write(pedaco)
            tam = os.path.getsize(parcial)
            if tamanho_esperado and tam != tamanho_esperado:
                raise IOError(f'tamanho divergente: {tam} != {tamanho_esperado}')
            os.replace(parcial, destino)
            mb = tam / 1e6
            log(f'baixado: {nome} ({mb:.0f} MB em {time.time() - inicio:.0f}s)')
            return destino
        except Exception as e:
            log(f'falha ao baixar {nome} (tentativa {tentativa}/{tentativas}): {e}')
            if tentativa == tentativas:
                raise
            time.sleep(5 * tentativa)


# --------------------------------------------------------------------------- #
# Filtros                                                                      #
# --------------------------------------------------------------------------- #
def montar_filtro(codigos, incluir_secundaria, ufs, situacoes):
    exatos = frozenset(c for c in codigos if len(c) == 7)
    prefixos = tuple(sorted(c for c in codigos if len(c) < 7))
    regex_sec = None
    if incluir_secundaria and codigos:
        alt = '|'.join([re.escape(c) for c in sorted(exatos)] +
                       [re.escape(p) + r'\d*' for p in prefixos])
        regex_sec = f'(?:^|,)(?:{alt})(?:,|$)'
    return {
        'exatos': exatos,
        'prefixos': prefixos,
        'regex_sec': regex_sec,
        'ufs': frozenset(ufs) if ufs else None,
        'situacoes': frozenset(situacoes) if situacoes else None,
    }


def assinatura_filtro(filtro):
    chave = repr((sorted(filtro['exatos']), filtro['prefixos'], filtro['regex_sec'],
                  sorted(filtro['ufs'] or []), sorted(filtro['situacoes'] or [])))
    return hashlib.sha1(chave.encode()).hexdigest()[:12]


def _mascara(df, filtro):
    principal = df['cnae_fiscal_principal']
    mask = principal.isin(filtro['exatos'])
    if filtro['prefixos']:
        mask = mask | principal.str.startswith(filtro['prefixos'])
    if filtro['regex_sec']:
        mask = mask | df['cnae_fiscal_secundaria'].str.contains(filtro['regex_sec'], regex=True)
    if filtro['ufs'] is not None:
        mask = mask & df['uf'].isin(filtro['ufs'])
    if filtro['situacoes'] is not None:
        mask = mask & df['situacao_cadastral'].str.zfill(2).isin(filtro['situacoes'])
    return mask


def _arquivos_do_zip(zf):
    return [n for n in zf.namelist() if not n.endswith('/')]


def _ler_em_blocos(fluxo, nomes, usar, chunk):
    return pd.read_csv(fluxo, sep=';', header=None, names=nomes, usecols=usar, dtype=str,
                       encoding='latin-1', quotechar='"', chunksize=chunk,
                       on_bad_lines='skip', na_filter=False, engine='c')


# --------------------------------------------------------------------------- #
# Workers (rodam em processos separados)                                       #
# --------------------------------------------------------------------------- #
def filtrar_estabelecimentos(tarefa):
    """Le um zip de estabelecimentos, mantem so as linhas do CNAE alvo."""
    zip_path, saida_csv, saida_npy, filtro, chunk = tarefa
    tag = os.path.basename(zip_path)
    lidas = mantidas = 0
    codigos = []

    with open(saida_csv, 'w', newline='', encoding='utf-8') as saida:
        with zipfile.ZipFile(zip_path) as zf:
            for interno in _arquivos_do_zip(zf):
                with zf.open(interno) as fluxo:
                    for df in _ler_em_blocos(fluxo, COLS_ESTAB, CAMPOS_ESTAB, chunk):
                        lidas += len(df)
                        df = df.loc[_mascara(df, filtro)]
                        if df.empty:
                            continue
                        df = df[CAMPOS_ESTAB].copy()
                        df.insert(0, 'cnpj', df['cnpj_basico'].str.zfill(8)
                                  + df['cnpj_ordem'].str.zfill(4)
                                  + df['cnpj_dv'].str.zfill(2))
                        df.to_csv(saida, sep=';', index=False, header=False,
                                  na_rep='', lineterminator='\n')
                        codigos.append(pd.to_numeric(df['cnpj_basico'], errors='coerce')
                                       .fillna(-1).astype('int64').to_numpy())
                        mantidas += len(df)

    arr = np.unique(np.concatenate(codigos)) if codigos else np.empty(0, dtype='int64')
    np.save(saida_npy, arr)
    return tag, lidas, mantidas


def filtrar_por_cnpj(tarefa):
    """Le um zip de empresas/socios, mantem so os cnpj_basico ja selecionados."""
    zip_path, saida_csv, npy_alvo, tipo, chunk = tarefa
    tag = os.path.basename(zip_path)
    alvo = np.load(npy_alvo)
    nomes, campos = (COLS_EMPRESA, SAIDA_EMPRESA) if tipo == 'empresa' else (COLS_SOCIOS, SAIDA_SOCIOS)
    lidas = mantidas = 0

    with open(saida_csv, 'w', newline='', encoding='utf-8') as saida:
        with zipfile.ZipFile(zip_path) as zf:
            for interno in _arquivos_do_zip(zf):
                with zf.open(interno) as fluxo:
                    for df in _ler_em_blocos(fluxo, nomes, campos, chunk):
                        lidas += len(df)
                        chaves = pd.to_numeric(df['cnpj_basico'], errors='coerce') \
                                   .fillna(-1).astype('int64')
                        df = df.loc[chaves.isin(alvo)]
                        if df.empty:
                            continue
                        df = df[campos].copy()
                        if tipo == 'empresa':
                            df['capital_social'] = pd.to_numeric(
                                df['capital_social'].str.replace(',', '.', regex=False),
                                errors='coerce')
                        df.to_csv(saida, sep=';', index=False, header=False,
                                  na_rep='', lineterminator='\n')
                        mantidas += len(df)
    return tag, lidas, mantidas


# --------------------------------------------------------------------------- #
# Dominios                                                                     #
# --------------------------------------------------------------------------- #
def ler_dominio(caminho_zip, zfill=0):
    """Le um zip de dominio e devolve {codigo: descricao}."""
    mapa = {}
    with zipfile.ZipFile(caminho_zip) as zf:
        for interno in _arquivos_do_zip(zf):
            with zf.open(interno) as fluxo:
                df = pd.read_csv(fluxo, sep=';', header=None, names=['codigo', 'descricao'],
                                 dtype=str, encoding='latin-1', quotechar='"', na_filter=False)
            for codigo, descricao in zip(df['codigo'], df['descricao']):
                mapa[codigo.zfill(zfill) if zfill else codigo] = descricao
    return mapa


def carregar_dominios(zips_por_nome):
    dominios = {}
    for zip_nome, (tabela, _) in DOMINIOS.items():
        caminho = zips_por_nome.get(zip_nome)
        if not caminho or not os.path.isfile(caminho):
            log(f'aviso: {zip_nome} indisponivel, descricoes de {tabela} ficarao vazias')
            dominios[tabela] = {}
            continue
        # qualificacoes aparecem com 1 ou 2 digitos nos dois lados do join
        dominios[tabela] = ler_dominio(caminho, zfill=2 if tabela == 'quals' else 0)
        log(f'dominio carregado: {tabela} ({len(dominios[tabela]):,} codigos)')
    return dominios


# --------------------------------------------------------------------------- #
# Montagem dos documentos                                                      #
# --------------------------------------------------------------------------- #
SITUACAO = {'01': 'Nula', '02': 'Ativa', '03': 'Suspensa', '04': 'Inapta', '08': 'Baixada'}
PORTE = {'01': 'Micro empresa', '03': 'Empresa de pequeno porte', '05': 'Demais'}
MATRIZ_FILIAL = {'1': 'Matriz', '2': 'Filial'}

# Naturezas de titular unico: nao ha registro em Socios, o proprietario e a
# propria razao social (empresario individual, EIRELI, produtor rural).
NATJU_TITULAR = frozenset({'2135', '2305', '4014'})

# Ordem de preferencia para eleger o socio "principal" do estabelecimento.
PRIORIDADE_QUAL = {'65': 0, '49': 1, '05': 2, '16': 3, '10': 4, '22': 5}
PRIORIDADE_PADRAO = 9


def _juntar(sep, *partes):
    return sep.join(p for p in partes if p) or None


def _data(bruto):
    if len(bruto) != 8 or bruto[0] not in '12' or not bruto.isdigit():
        return None
    try:
        return datetime.datetime.strptime(bruto, '%Y%m%d')
    except ValueError:
        return None


def formatar_cnpj(cnpj):
    if len(cnpj) != 14:
        return None
    return f'{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}'


def _telefone(ddd, numero):
    if not numero:
        return None
    return f'({ddd}) {numero}' if ddd else numero


def _cnaes_secundarios(bruto, cnae):
    saida = []
    for codigo in bruto.split(','):
        codigo = codigo.strip()
        if codigo and codigo != '0':
            saida.append({'codigo': codigo, 'descricao': cnae.get(codigo)})
    return saida


def _ler_partes(arquivos, nomes):
    pedacos = [pd.read_csv(caminho, sep=';', header=None, names=nomes, dtype=str,
                           encoding='utf-8', quotechar='"', na_filter=False)
               for caminho in sorted(arquivos)
               if os.path.isfile(caminho) and os.path.getsize(caminho) > 0]
    if not pedacos:
        return pd.DataFrame({n: pd.Series(dtype='object') for n in nomes})
    return pd.concat(pedacos, ignore_index=True)


def carregar_empresas(arquivos):
    """Partes filtradas de empresas -> DataFrame indexado por cnpj_basico."""
    df = _ler_partes(arquivos, SAIDA_EMPRESA)
    df['capital_social'] = pd.to_numeric(df['capital_social'], errors='coerce')
    return df.drop_duplicates('cnpj_basico').set_index('cnpj_basico')


def carregar_socios(arquivos):
    """Partes filtradas de socios -> DataFrame indexado por cnpj_basico.

    Ordenado por relevancia da qualificacao, entao o primeiro registro de cada
    cnpj_basico e o socio que representa a empresa.
    """
    df = _ler_partes(arquivos, SAIDA_SOCIOS)
    prioridade = df['qualificacao_socio'].str.zfill(2).map(PRIORIDADE_QUAL)
    df = df.assign(_prio=prioridade.fillna(PRIORIDADE_PADRAO).astype('int8'))
    df = df.sort_values(['cnpj_basico', '_prio', 'data_entrada_sociedade'], kind='stable')
    return df.set_index('cnpj_basico')


def socios_do_bloco(socios, chaves, quals):
    """Agrupa os socios dos cnpj_basico do bloco em listas de subdocumentos."""
    if socios.empty:
        return {}
    presentes = socios.index.intersection(pd.unique(chaves))
    if len(presentes) == 0:
        return {}

    sub = socios.loc[presentes]
    agrupado = {}
    for basico, nome, qual, cpf, entrada in zip(
            sub.index, sub['nome_socio_razao_social'], sub['qualificacao_socio'],
            sub['cpf_cnpj_socio'], sub['data_entrada_sociedade']):
        agrupado.setdefault(basico, []).append({
            'nome': nome or None,
            'qualificacao': quals.get(qual.zfill(2)) if qual else None,
            'qualificacao_codigo': qual.zfill(2) if qual else None,
            'cpf_cnpj': cpf or None,
            'data_entrada': _data(entrada),
        })
    return agrupado


def montar_documentos(bloco, empresas, socios, dominios):
    """Une estabelecimento + empresa + socios + dominios num documento por CNPJ."""
    cnae, munic = dominios['cnae'], dominios['munic']
    natju, quals = dominios['natju'], dominios['quals']

    bloco = bloco.join(empresas, on='cnpj_basico')
    textuais = [c for c in SAIDA_EMPRESA if c not in ('cnpj_basico', 'capital_social')]
    bloco[textuais] = bloco[textuais].fillna('')
    por_cnpj = socios_do_bloco(socios, bloco['cnpj_basico'].to_numpy(), quals)

    documentos = []
    for linha in bloco.itertuples(index=False):
        lista_socios = por_cnpj.get(linha.cnpj_basico, [])
        if lista_socios:
            proprietario = lista_socios[0]['nome']
            qualificacao = lista_socios[0]['qualificacao']
        elif linha.natureza_juridica in NATJU_TITULAR:
            proprietario, qualificacao = linha.razao_social or None, None
        else:
            proprietario = qualificacao = None

        municipio = munic.get(linha.municipio)
        logradouro = _juntar(' ', linha.tipo_logradouro, linha.logradouro, linha.numero)
        cep = f'CEP {linha.cep[:5]}-{linha.cep[5:]}' if len(linha.cep) == 8 else None

        documentos.append({
            '_id': linha.cnpj,
            'cnpj': linha.cnpj,
            'cnpj_formatado': formatar_cnpj(linha.cnpj),
            'cnpj_basico': linha.cnpj_basico,
            'razao_social': linha.razao_social or None,
            'nome_fantasia': linha.nome_fantasia or None,
            'nome_proprietario': proprietario,
            'qualificacao_proprietario': qualificacao,
            'socios': lista_socios,
            'telefone_1': _telefone(linha.ddd_1, linha.telefone_1),
            'telefone_2': _telefone(linha.ddd_2, linha.telefone_2),
            'email': linha.correio_eletronico.lower() or None,
            'endereco_completo': _juntar(', ', logradouro, linha.complemento, linha.bairro,
                                         _juntar('/', municipio, linha.uf), cep),
            'endereco': {
                'tipo_logradouro': linha.tipo_logradouro or None,
                'logradouro': linha.logradouro or None,
                'numero': linha.numero or None,
                'complemento': linha.complemento or None,
                'bairro': linha.bairro or None,
                'cep': linha.cep or None,
                'municipio': municipio,
                'municipio_codigo': linha.municipio or None,
                'uf': linha.uf or None,
            },
            'cnae_principal': {
                'codigo': linha.cnae_fiscal_principal or None,
                'descricao': cnae.get(linha.cnae_fiscal_principal),
            },
            'cnae_secundarios': _cnaes_secundarios(linha.cnae_fiscal_secundaria, cnae),
            'matriz_filial': MATRIZ_FILIAL.get(linha.identificador_matriz_filial),
            'situacao_cadastral': SITUACAO.get(linha.situacao_cadastral.zfill(2)),
            'data_situacao_cadastral': _data(linha.data_situacao_cadastral),
            'data_inicio_atividade': _data(linha.data_inicio_atividade),
            'porte_empresa': PORTE.get(linha.porte_empresa.zfill(2)),
            'natureza_juridica': natju.get(linha.natureza_juridica),
            'natureza_juridica_codigo': linha.natureza_juridica or None,
            'capital_social': None if pd.isna(linha.capital_social) else float(linha.capital_social),
        })
    return documentos


# --------------------------------------------------------------------------- #
# Saidas: NDJSON e MongoDB                                                     #
# --------------------------------------------------------------------------- #
INDICES_MONGO = [
    [('cnpj_basico', ASCENDING)],
    [('cnae_principal.codigo', ASCENDING)],
    [('endereco.uf', ASCENDING), ('endereco.municipio', ASCENDING)],
    [('situacao_cadastral', ASCENDING)],
]


def gerar_documentos(arquivos, empresas, socios, dominios, chunk):
    for caminho in sorted(arquivos):
        if not os.path.isfile(caminho) or os.path.getsize(caminho) == 0:
            continue
        leitor = pd.read_csv(caminho, sep=';', header=None, names=SAIDA_ESTAB, dtype=str,
                             encoding='utf-8', quotechar='"', na_filter=False, chunksize=chunk)
        for bloco in leitor:
            yield from montar_documentos(bloco, empresas, socios, dominios)


def tee_ndjson(documentos, caminho):
    """Grava cada documento como uma linha de Extended JSON e repassa adiante."""
    abrir = gzip.open if caminho.endswith('.gz') else open
    with abrir(caminho, 'wt', encoding='utf-8') as saida:
        for doc in documentos:
            saida.write(json_util.dumps(doc))
            saida.write('\n')
            yield doc


def inserir_mongo(documentos, uri, nome_db, nome_colecao, lote, swap):
    """Insere em lotes, indexa e so entao troca pela collection definitiva."""
    cliente = MongoClient(uri)
    db = cliente.get_database(nome_db)
    destino = f'{nome_colecao}__carga' if swap else nome_colecao
    db[destino].drop()

    total = 0
    buffer = []
    for doc in documentos:
        buffer.append(doc)
        if len(buffer) >= lote:
            db[destino].insert_many(buffer, ordered=False)
            total += len(buffer)
            buffer = []
            log(f'mongo: {total:,} documentos inseridos')
    if buffer:
        db[destino].insert_many(buffer, ordered=False)
        total += len(buffer)

    # Indices so depois da carga: construi-los durante o insert e o que pesa no servidor.
    for chaves in INDICES_MONGO:
        db[destino].create_index(chaves)
    if swap:
        db[destino].rename(nome_colecao, dropTarget=True)
    cliente.close()
    return total


# --------------------------------------------------------------------------- #
# Configuracao                                                                 #
# --------------------------------------------------------------------------- #
def ler_cnaes(inline, arquivo):
    codigos = set()
    for bruto in (inline or '').replace(';', ',').split(','):
        limpo = re.sub(r'\D', '', bruto)
        if limpo:
            codigos.add(limpo)
    if arquivo and os.path.isfile(arquivo):
        with open(arquivo, encoding='utf-8') as f:
            for linha in f:
                limpo = re.sub(r'\D', '', linha.split('#')[0])
                if limpo:
                    codigos.add(limpo)
    return sorted(codigos)


def env(chave, padrao=None):
    valor = os.getenv(chave)
    return padrao if valor is None or valor == '' else valor


def env_bool(chave, padrao):
    return env(chave, '1' if padrao else '0').strip().lower() in ('1', 'true', 'sim', 'yes')


def env_lista(chave):
    return [v.strip().upper() for v in (env(chave, '') or '').split(',') if v.strip()]


# --------------------------------------------------------------------------- #
# Pipeline                                                                     #
# --------------------------------------------------------------------------- #
def main():
    inicio = time.time()
    base = pathlib.Path(__file__).resolve().parent
    dotenv_path = base / '.env'
    if not dotenv_path.is_file():
        sys.exit(f'Arquivo ".env" nao encontrado em {dotenv_path}. Copie ".env_template".')
    load_dotenv(dotenv_path=dotenv_path)

    zips_dir = env('OUTPUT_FILES_PATH', str(base.parent / 'data' / 'output_files'))
    partes_dir = env('PARTES_FILES_PATH', str(pathlib.Path(zips_dir).parent / 'partes_filtradas'))
    makedirs(zips_dir)
    makedirs(partes_dir)

    cnaes = ler_cnaes(env('CNAE_LIST'), env('CNAE_ARQUIVO', str(base / 'cnaes.txt')))
    if not cnaes:
        sys.exit('Nenhum CNAE configurado. Preencha CNAE_LIST no .env ou code/cnaes.txt.')

    filtro = montar_filtro(
        cnaes,
        env_bool('CNAE_INCLUIR_SECUNDARIA', True),
        env_lista('UF_LIST'),
        [s.zfill(2) for s in env_lista('SITUACAO_CADASTRAL')],
    )
    assinatura = assinatura_filtro(filtro)

    chunk = int(env('CHUNK_LINHAS', '100000'))
    proc_workers = int(env('PROC_WORKERS', str(min(6, os.cpu_count() or 4))))
    dl_workers = int(env('DOWNLOAD_WORKERS', '4'))
    apagar_zips = env_bool('APAGAR_ZIPS', False)
    reprocessar = env_bool('REPROCESSAR', False)
    ndjson_path = env('EXPORT_NDJSON_PATH',
                      str(pathlib.Path(zips_dir).parent / 'empresas_cnae.ndjson.gz'))
    if ndjson_path.strip().lower() in ('0', 'nao', 'no', 'false'):
        ndjson_path = None
    mongo_uri = os.getenv('MONGO_URI') or None
    mongo_db = env('MONGO_DB')
    mongo_colecao = env('MONGO_COLLECTION', 'reseller_shop')
    mongo_lote = int(env('MONGO_BATCH', '5000'))
    mongo_swap = env_bool('MONGO_SWAP', True)
    if not ndjson_path and not mongo_uri:
        sys.exit('Nenhuma saida configurada: defina MONGO_URI e/ou EXPORT_NDJSON_PATH no .env.')

    mes = descobrir_mes(env('MES_REFERENCIA'))
    log(f'mes de referencia: {mes}')
    log(f'CNAEs alvo ({len(cnaes)}): {", ".join(cnaes[:20])}{" ..." if len(cnaes) > 20 else ""}')
    log(f'CNAE secundario: {"sim" if filtro["regex_sec"] else "nao"} | '
        f'UFs: {",".join(sorted(filtro["ufs"])) if filtro["ufs"] else "todas"} | '
        f'situacao: {",".join(sorted(filtro["situacoes"])) if filtro["situacoes"] else "todas"}')

    remotos = {i['name']: i['size'] for i in webdav_list(f'/{mes}/') if not i['is_dir']}
    estab = sorted(n for n in remotos if n.startswith('Estabelecimentos'))
    empresas = sorted(n for n in remotos if n.startswith('Empresas'))
    socios = sorted(n for n in remotos if n.startswith('Socios'))
    zips_dominio = [n for n in DOMINIOS if n in remotos]
    necessarios = estab + empresas + socios + zips_dominio  # Simples.zip nao e baixado

    total_mb = sum(remotos[n] for n in necessarios) / 1e6
    log(f'{len(necessarios)} arquivos a garantir ({total_mb:.0f} MB) - Simples.zip ignorado')

    caminho_de = {n: os.path.join(zips_dir, n) for n in necessarios}
    marcador_de = {n: os.path.join(partes_dir, f'{n}.ok') for n in necessarios}

    def ja_processado(nome):
        marcador = marcador_de[nome]
        if reprocessar or not os.path.isfile(marcador) or not os.path.isfile(parte(nome)):
            return False
        with open(marcador) as f:
            return f.read().strip() == f'{assinatura}:{remotos[nome]}'

    def marcar(nome):
        with open(marcador_de[nome], 'w') as f:
            f.write(f'{assinatura}:{remotos[nome]}')

    def parte(nome):
        return os.path.join(partes_dir, f'{nome}.csv')

    def npy(nome):
        return os.path.join(partes_dir, f'{nome}.npy')

    def descartar(nome):
        if apagar_zips and os.path.isfile(caminho_de[nome]):
            os.remove(caminho_de[nome])

    with ThreadPoolExecutor(max_workers=dl_workers) as rede, \
            ProcessPoolExecutor(max_workers=proc_workers) as cpu:

        downloads = {}
        for nome in necessarios:
            if ja_processado(nome) and nome not in DOMINIOS:
                log(f'ja filtrado anteriormente: {nome}')
                continue
            downloads[rede.submit(baixar, nome, mes, caminho_de[nome], remotos[nome])] = nome

        # ---- Estabelecimentos: filtra por CNAE assim que cada zip termina ----
        log('--- fase 1: estabelecimentos (filtro por CNAE) ---')
        futuros_estab = {}
        pendentes = [f for f, n in downloads.items() if n in estab]
        for fut in as_completed(pendentes):
            nome = downloads[fut]
            fut.result()
            tarefa = (caminho_de[nome], parte(nome), npy(nome), filtro, chunk)
            futuros_estab[cpu.submit(filtrar_estabelecimentos, tarefa)] = nome

        lidas = mantidas = 0
        for fut in as_completed(futuros_estab):
            nome = futuros_estab[fut]
            tag, li, ma = fut.result()
            lidas += li
            mantidas += ma
            marcar(nome)
            descartar(nome)
            log(f'{tag}: {li:,} linhas lidas -> {ma:,} mantidas')

        arrays = [np.load(npy(n)) for n in estab if os.path.isfile(npy(n))]
        alvo = np.unique(np.concatenate(arrays)) if arrays else np.empty(0, dtype='int64')
        npy_alvo = os.path.join(partes_dir, 'cnpj_alvo.npy')
        np.save(npy_alvo, alvo)
        log(f'estabelecimentos: {lidas:,} lidos, {mantidas:,} no filtro, '
            f'{len(alvo):,} empresas distintas')
        if len(alvo) == 0:
            log('ATENCAO: nenhum estabelecimento bateu com o filtro. Confira a lista de CNAEs.')

        # ---- Empresas e socios: filtra pelos cnpj_basico selecionados ----
        log('--- fase 2: empresas e socios (filtro por cnpj_basico) ---')
        for fut in as_completed([f for f, n in downloads.items() if n in empresas + socios]):
            fut.result()

        futuros_cnpj = {}
        for nome in empresas + socios:
            if ja_processado(nome):
                continue
            tipo = 'empresa' if nome in empresas else 'socios'
            tarefa = (caminho_de[nome], parte(nome), npy_alvo, tipo, chunk)
            futuros_cnpj[cpu.submit(filtrar_por_cnpj, tarefa)] = nome

        for fut in as_completed(futuros_cnpj):
            nome = futuros_cnpj[fut]
            tag, li, ma = fut.result()
            marcar(nome)
            descartar(nome)
            log(f'{tag}: {li:,} linhas lidas -> {ma:,} mantidas')

        for fut in as_completed([f for f, n in downloads.items() if n in DOMINIOS]):
            fut.result()

    # ---- Montagem dos documentos e entrega ----
    log('--- fase 3: montagem dos documentos ---')
    dominios = carregar_dominios(caminho_de)
    empresas_df = carregar_empresas([parte(n) for n in empresas])
    socios_df = carregar_socios([parte(n) for n in socios])
    log(f'em memoria: {len(empresas_df):,} empresas, {len(socios_df):,} vinculos de socios')

    documentos = gerar_documentos([parte(n) for n in estab], empresas_df, socios_df,
                                  dominios, chunk)
    if ndjson_path:
        documentos = tee_ndjson(documentos, ndjson_path)

    if mongo_uri:
        log(f'--- fase 4: carga no MongoDB (collection {mongo_colecao}) ---')
        total = inserir_mongo(documentos, mongo_uri, mongo_db, mongo_colecao,
                              mongo_lote, mongo_swap)
    else:
        total = sum(1 for _ in documentos)

    log(f'{total:,} documentos gerados')
    if ndjson_path:
        log(f'NDJSON: {ndjson_path} ({os.path.getsize(ndjson_path) / 1e6:.0f} MB)')
    if mongo_uri:
        log(f'MongoDB: collection {mongo_colecao} pronta')
    log(f'concluido em {round(time.time() - inicio)}s')


if __name__ == '__main__':
    main()
