# Dados Públicos CNPJ
- Fonte oficial da Receita Federal do Brasil, [aqui](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj).
- Layout dos arquivos, [aqui](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf).

A Receita Federal do Brasil disponibiliza bases com os dados públicos do cadastro nacional de pessoas jurídicas (CNPJ).

De forma geral, nelas constam as mesmas informações que conseguimos ver no cartão do CNPJ, quando fazemos uma consulta individual, acrescidas de outros dados de Simples Nacional, sócios e etc. Análises muito ricas podem sair desses dados, desde econômicas, mercadológicas até investigações.

Nesse repositório consta um processo de ETL para **i)** baixar os arquivos; **ii)** filtrar
pelos CNAEs de interesse; **iii)** montar um documento por CNPJ e **iv)** gravar no MongoDB.

| Script | O que faz | Destino | Tempo típico |
|---|---|---|---|
| `code/ETL_cnae_filtrado.py` | Carrega apenas os CNPJs dos CNAEs que você escolher | MongoDB / NDJSON | ~25–30 min |

### Infraestrutura necessária
- Python 3.10+
- MongoDB acessível (local ou remoto) — informado em `MONGO_URI`
- ~10 GB livres em disco durante a execução

---------------------

## ETL filtrado por CNAE (`ETL_cnae_filtrado.py`)

Feito para quem precisa só de um recorte da base: informe uma lista de CNAEs e o
processo traz apenas os CNPJs daquelas atividades, com **razão social, nome do
proprietário, CNPJ, telefone, e-mail e endereço completo**, já como documentos prontos
para o MongoDB.

### Por que é muito mais rápido

- **Filtra antes de gravar**: as linhas são descartadas durante a leitura do CSV, então só o
  recorte é processado (o processo antigo carregava ~74 milhões de estabelecimentos
  para depois filtrar).
- **Não descompacta nada em disco**: os CSVs são lidos de dentro do `.zip` em streaming.
  Some a etapa de extrair ~25 GB.
- **Não baixa o que não é usado**: `Simples.zip` é ignorado.
- **Paralelismo**: downloads simultâneos e filtragem multi-processo; o filtro de um arquivo
  já começa enquanto os outros ainda estão baixando.

### Onde cada etapa roda

A fase pesada (download, filtragem, joins) foi desenhada para rodar numa **instância
separada** — sua máquina ou uma VM descartável. O servidor de produção não vê os `.zip`,
não descompacta nada e não roda pandas: ele só recebe os documentos prontos.

Duas formas de entregar, e as duas podem ser usadas ao mesmo tempo:

- **`MONGO_URI`** — a própria instância que fez a mineração insere direto na collection.
  `mongoimport`/pymongo são clientes: não precisam rodar no servidor, basta alcançá-lo.
- **`EXPORT_NDJSON_PATH`** — gera um `.ndjson.gz` (um documento por linha, formato nativo
  do `mongoimport`). Útil quando a instância não alcança o Mongo: você transfere o arquivo
  e importa de onde quiser.

```bash
mongoimport --uri "mongodb://usuario:senha@host:27017/meubanco?authSource=admin" \
            --collection reseller_shop --type json \
            --gzip --file data/empresas_cnae.ndjson.gz \
            --mode upsert --numInsertionWorkers 2
```

`--mode upsert` porque o `_id` de cada documento é o próprio CNPJ: reimportar atualiza em
vez de duplicar. `--numInsertionWorkers 2` segura a carga no servidor.

Medido nesta base, numa máquina de 12 núcleos:

| Fase | Tempo | Taxa medida |
|---|---:|---|
| Download de 7,4 GB (4 conexões) | ~21 min | 5,9 MB/s agregados |
| Filtrar 72,8 M estabelecimentos | ~3–4 min | 0,20–0,35 M linhas/s por processo |
| Filtrar empresas + sócios | ~2–3 min | — |
| Montar os documentos | ~26 s | 26.000 docs/s |
| Inserir no Mongo + índices | ~110 s | 7.000 docs/s |

O download domina, e a filtragem roda em paralelo com ele. Não adianta subir
`DOWNLOAD_WORKERS` acima de 4: com 8 conexões o servidor da Receita começa a dar timeout.
Reexecutando com os `.zip` já em disco são ~6–8 min; com as partes filtradas em cache
(mesma lista de CNAEs), só a montagem e a carga, ~2,5 min.

### Como usar

1. Copie `code/.env_template` para `code/.env` e ajuste as variáveis: diretórios, opções de
   filtro/performance e pelo menos uma saída (`MONGO_URI` e/ou `EXPORT_NDJSON_PATH`).

2. Coloque a sua lista de CNAEs em `code/cnaes.txt` (um por linha) e/ou em `CNAE_LIST` no
   `.env`. Pontuação é ignorada, então `5611-2/01` e `5611201` são equivalentes:
   - código com **7 dígitos** → casa exatamente aquele CNAE;
   - código com **menos de 7 dígitos** → casa por prefixo (`5611` pega `5611201`, `5611202`,
     `5611203`; `62` pega todo o grupo de TI).

3. Instale as dependências e execute:
```
pip install -r requirements.txt
python code/ETL_cnae_filtrado.py
```

### Opções principais do `.env`

| Variável | Padrão | Para que serve |
|---|---|---|
| `CNAE_LIST` / `CNAE_ARQUIVO` | `code/cnaes.txt` | Lista de CNAEs alvo |
| `CNAE_INCLUIR_SECUNDARIA` | `1` | Também traz quem tem o CNAE como atividade secundária |
| `UF_LIST` | vazio | Restringe por UF (ex.: `SP,MG`) |
| `SITUACAO_CADASTRAL` | vazio | Ex.: `02` para trazer só empresas ativas |
| `PROC_WORKERS` | `6` | Processos de filtragem em paralelo |
| `DOWNLOAD_WORKERS` | `4` | Downloads simultâneos |
| `APAGAR_ZIPS` | `0` | `1` apaga cada `.zip` depois de filtrar (economiza disco) |
| `REPROCESSAR` | `0` | `1` ignora o cache e refaz a filtragem |
| `LIMPAR_TEMPORARIOS` | `1` | Apaga os `.zip` e os CSVs intermediários no fim da carga |
| `EXPORT_NDJSON_PATH` | `data/empresas_cnae.ndjson.gz` | Arquivo NDJSON gerado (`0` = não gera) |
| `MONGO_URI` | vazio | Se preenchido, insere direto na collection |
| `MONGO_DB` | banco da URI | Sobrescreve o banco de destino |
| `MONGO_COLLECTION` | `reseller_shop` | Collection de destino |
| `MONGO_BATCH` | `5000` | Documentos por `insert_many` |
| `MONGO_SWAP` | `1` | Carrega numa collection temporária e só troca no fim |

O processo é **retomável**: cada `.zip` já filtrado ganha um marcador em
`data/partes_filtradas/`. Se você rodar de novo com a **mesma** lista de CNAEs, ele pula o
que já foi feito; se a lista mudar, ele refaz automaticamente.

Com `LIMPAR_TEMPORARIOS=1` (padrão) o `data/` fica vazio no fim: os `.zip` e os CSVs
intermediários são apagados assim que a carga termina, para não sujar o repositório do
projeto. O efeito colateral é que isso **descarta o cache de retomada** — a execução
seguinte volta a baixar os 7,4 GB. Se estiver iterando na lista de CNAEs, use
`LIMPAR_TEMPORARIOS=0` enquanto testa. A limpeza só roda se a carga terminar sem erro;
se algo falhar no meio, o cache é preservado para você retomar.

### Resultado

Um documento por estabelecimento, com empresa, sócios e descrições de domínio já
resolvidos — nada de código numérico solto ou join pendente. O `_id` é o próprio CNPJ:

```js
{
  "_id": "11111111000101",
  "cnpj": "11111111000101",
  "razao_social": "ZE ALIMENTOS LTDA",
  "nome_fantasia": "PADARIA DO ZE",
  "nome_proprietario": "ZE ADMINISTRADOR",
  "socios": [
    { "nome": "ZE ADMINISTRADOR" }
  ],
  "telefone_1": "(11) 999998888",
  "email": "contato@ze.com.br",
  "endereco": {
    "tipo_logradouro": "RUA",
    "logradouro": "DAS FLORES",
    "numero": "100",
    "complemento": "SALA 2",
    "bairro": "CENTRO",
    "cep": "01310100",
    "municipio": "SÃO PAULO",
    "uf": "SP"
  },
  "cnae_principal": { "codigo": "5611201", "descricao": "Restaurantes e similares" },
  "cnae_secundarios": [ { "codigo": "4712100", "descricao": "Minimercados..." } ],
  "situacao_cadastral": "Ativa",
  "porte_empresa": "Empresa de pequeno porte",
  "capital_social": 150000.0
}
```

`socios` e `cnae_secundarios` são **arrays de subdocumentos** — dá para consultar com
`$elemMatch` em vez de fazer `LIKE` numa string concatenada. `capital_social` é numérico,
então filtros de intervalo funcionam direto:

```js
db.reseller_shop.find({
  "endereco.uf": "SP",
  "situacao_cadastral": "Ativa",
  "cnae_principal.codigo": "5611201"
})
```

Índices criados **depois** da carga (construí-los durante o insert é o que pesa no
servidor): `cnpj_basico`, `cnae_principal.codigo`, `endereco.uf + endereco.municipio` e
`situacao_cadastral`.

`nome_proprietario` usa o sócio de maior relevância (titular, sócio-administrador,
administrador, presidente, diretor, nessa ordem) e cai para a razão social quando a empresa
é um empresário individual / MEI, que não tem quadro societário.

Com `MONGO_SWAP=1` (padrão) a carga vai para uma collection temporária e só troca pela
definitiva no final, então a aplicação nunca lê dados pela metade.

---------------------
