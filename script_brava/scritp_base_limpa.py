id_crediario = """
-- QUAL O ID DO CREDIARIO ESCOLHIDO 
SELECT
*
FROM dblink('conexao_baseorigem', '
SELECT 
id,
nome
FROM 
crediario
where (nome ilike ''%NOTINHA%''or nome ilike ''%VANTAGENS%'')
') AS DATA (id BIGINT, nome VARCHAR);
"""

id_agg_crediario = """
WITH CREDIARIO_ID AS ( 
SELECT
	*
FROM dblink('conexao_baseorigem', '
SELECT 
	id,
	nome
	FROM 
	crediario
where nome ilike ''%NOTINHA%''
') AS DATA (id BIGINT, nome VARCHAR))
SELECT
	string_agg(crediario_id.id::varchar,',')
from
CREDIARIO_ID
"""

id_cadernooferta = """
SELECT
  *
FROM dblink('conexao_baseorigem', '
SELECT 
  cadernooferta.ID,
  cadernooferta.Nome 
FROM 
  cadernooferta 
WHERE (nome ilike ''%NOTINHA%''or nome ilike ''%VANTAGENS%'')
	and status = ''A''
') AS DATA (id BIGINT, nome VARCHAR);
"""

id_agg_caderno = """
WITH CADERNO_ID AS ( 
SELECT
  *
FROM dblink('conexao_baseorigem', '
SELECT 
  cadernooferta.ID,
  cadernooferta.Nome 
FROM 
  cadernooferta 
WHERE 
 nome ilike ''%- NOTINHA%''
	and status = ''A''
') AS DATA (id BIGINT, nome VARCHAR))
SELECT
	string_agg(CADERNO_ID.id::varchar,',')
from CADERNO_ID
"""

id_loja = """
SELECT
  *
FROM dblink('conexao_baseorigem', '
SELECT 
  ID,
  codigo,
  Nome 
FROM 
  unidadenegocio 
WHERE 
	status = ''A''
ORDER BY 2
') AS DATA (id BIGINT, CODIGO VARCHAR ,nome VARCHAR)
"""

planopagamento_brava = """
--BASE LIMPA 
WITH planoremuneracaobrava AS (
SELECT
  *
FROM dblink('conexao_baseorigem', '
 SELECT 
  planopagamento.id,
  planopagamento.nome,
  planopagamento.minparcela,
  planopagamento.maxparcela,
  planopagamento.tipointervaloentrada,
  planopagamento.intervaloentrada,
  planopagamento.tipointervaloparcela,
  planopagamento.intervaloparcela,
  planopagamento.tipopagamento,
  fechamentoplanopagamento.diafechamento,
  fechamentoplanopagamento.diavencimento 
FROM planopagamento
LEFT JOIN fechamentoplanopagamento ON fechamentoplanopagamento.planopagamentoid = planopagamento.id
JOIN crediario ON crediario.planopagamentoid = planopagamento.id
WHERE crediario.id in ({ids}) --ID DO CREDIARIO ESCOLHIDO PELO CLIENTE 
') AS DATA (id BIGINT, nome VARCHAR, minparcela INTEGER, maxparcela INTEGER, tipointervaloentrada VARCHAR, intervaloentrada INTEGER,
tipointervaloparcela VARCHAR,intervaloparcela VARCHAR,tipopagamento VARCHAR, diafechamento INTEGER, diavencimento INTEGER)
)
SELECT 
'select nq(''insert into planopagamento (id,nome,minparcela,maxparcela,tipointervaloentrada,intervaloentrada,tipointervaloparcela,intervaloparcela,tipopagamento ) 
values ('||generate_id()||','''''||nome||''''','||minparcela||','||maxparcela||','''''||tipointervaloentrada||''''','||intervaloentrada||',
'''''||tipointervaloparcela||''''','||intervaloparcela||','''''||tipopagamento||''''')'');'
FROM planoremuneracaobrava"""

planopagamento_brava_2 = """
--BASE LIMPA 
WITH planoremuneracaobrava AS (
SELECT
  *
FROM dblink('conexao_baseorigem', '
 SELECT 
  planopagamento.id,
  planopagamento.nome,
  planopagamento.minparcela,
  planopagamento.maxparcela,
  planopagamento.tipointervaloentrada,
  planopagamento.intervaloentrada,
  planopagamento.tipointervaloparcela,
  planopagamento.intervaloparcela,
  planopagamento.tipopagamento,
  fechamentoplanopagamento.diafechamento,
  fechamentoplanopagamento.diavencimento 
FROM planopagamento
LEFT JOIN fechamentoplanopagamento ON fechamentoplanopagamento.planopagamentoid = planopagamento.id
JOIN crediario ON crediario.planopagamentoid = planopagamento.id
WHERE crediario.id in ({ids})  --ID DO CREDIARIO ESCOLHIDO PELO CLIENTE 
') AS DATA (id BIGINT, nome VARCHAR, minparcela INTEGER, maxparcela INTEGER, tipointervaloentrada VARCHAR, intervaloentrada INTEGER,
tipointervaloparcela VARCHAR,intervaloparcela VARCHAR,tipopagamento VARCHAR, diafechamento INTEGER, diavencimento INTEGER)
)
SELECT 
'select nq(''insert into planopagamento (id,nome,minparcela,maxparcela,tipointervaloentrada,intervaloentrada,tipointervaloparcela,intervaloparcela,tipopagamento ) 
values ('||generate_id()||','''''||nome||''''','||minparcela||','||maxparcela||','''''||tipointervaloentrada||''''','||intervaloentrada||',
'''''||tipointervaloparcela||''''','||intervaloparcela||','''''||tipopagamento||''''')'');'
FROM planoremuneracaobrava
"""

crediario_brava = """
--CREDIARIO INSERT
WITH crediario_brava AS (
SELECT
  *
FROM dblink('conexao_baseorigem', '
SELECT 
crediario.*,
planopagamento.nome as planoid
FROM 
  crediario
  join planopagamento on planopagamento.id = crediario.planopagamentoid
  where crediario.id in ({ids})  -- ID DO CREDIARIO ESCOLHIDO PELO CLIENTE 
') AS DATA (id bigint, status varchar, nome varchar, limitepadraocliente numeric (15,4), tabelarestricoesid bigint, pessoaid bigint, mensagemvenda varchar, planopagamentoid bigint, taxajuros numeric (15,4), taxamulta numeric (15,4), taxaconvenio numeric (15,4), toleranciaatraso integer, tipo varchar, recebimentominimo integer, autorizacao varchar, exigirsenhaoubiometriacliente boolean, toleranciajuros integer, toleranciamulta integer, layoutexportacao varchar, layoutimportacaocliente varchar, regrafaturamentoid bigint, comprovanteimprimirviaparcela boolean, comprovanteimprimiritemvenda boolean, comprovanteporcentagemtitular numeric (15,4), comprovanteporcentagemdependente numeric (15,4), desconsiderarcompra boolean, numerocartaoexportacaounico boolean, codigoexportacao varchar, tabelarestricoestrocaid bigint, usuariowebconvenios varchar, senhawebconvenios varchar, tipocalculolimite varchar, cadernoofertaid bigint, codigoimportacao varchar, bloquearbuscamanual boolean, integracaocontabilcodigo varchar, emitecartaointerno boolean, comprovanteimprimirsaldorestante boolean, layoutexportacaoarquivoid bigint, plataformatef varchar,planoid VARCHAR)
)SELECT 
   'select nq(''insert into crediario(id,status,nome,limitepadraocliente,planopagamentoid,taxajuros,taxamulta,taxaconvenio,toleranciaatraso,tipo,recebimentominimo,autorizacao,toleranciajuros,toleranciamulta,layoutexportacao,layoutimportacaocliente,comprovanteimprimirviaparcela,comprovanteimprimiritemvenda,desconsiderarcompra,numerocartaoexportacaounico,tipocalculolimite,bloquearbuscamanual,emitecartaointerno,comprovanteimprimirsaldorestante) 
   values('||generate_id()||','''''||STATUS||''''','''''||crediario_brava.nome||''''','||limitepadraocliente||','||planopagamento.id||','||taxajuros||','||taxamulta||','||taxaconvenio||','||toleranciaatraso||','''''||tipo||''''','||recebimentominimo||','''''||autorizacao||''''','||toleranciajuros||','||toleranciamulta||','''''||layoutexportacao||''''','''''||layoutimportacaocliente||''''','||comprovanteimprimirviaparcela||','||comprovanteimprimiritemvenda||','||desconsiderarcompra||','||numerocartaoexportacaounico||','''''||tipocalculolimite||''''','||bloquearbuscamanual||','||emitecartaointerno||','||comprovanteimprimirsaldorestante||')'');'
FROM crediario_brava
JOIN planopagamento ON planopagamento.nome = crediario_brava.planoid
"""

crediario_brava_2= """
--CREDIARIO INSERT
WITH crediario_brava AS (
SELECT
  *
FROM dblink('conexao_baseorigem', '
SELECT 
crediario.*,
planopagamento.nome as planoid
FROM 
  crediario
  join planopagamento on planopagamento.id = crediario.planopagamentoid
  where crediario.id in ({ids})  -- ID DO CREDIARIO ESCOLHIDO PELO CLIENTE 
') AS DATA (id bigint, status varchar, nome varchar, limitepadraocliente numeric (15,4), tabelarestricoesid bigint, pessoaid bigint, mensagemvenda varchar, planopagamentoid bigint, taxajuros numeric (15,4), taxamulta numeric (15,4), taxaconvenio numeric (15,4), toleranciaatraso integer, tipo varchar, recebimentominimo integer, autorizacao varchar, exigirsenhaoubiometriacliente boolean, toleranciajuros integer, toleranciamulta integer, layoutexportacao varchar, layoutimportacaocliente varchar, regrafaturamentoid bigint, comprovanteimprimirviaparcela boolean, comprovanteimprimiritemvenda boolean, comprovanteporcentagemtitular numeric (15,4), comprovanteporcentagemdependente numeric (15,4), desconsiderarcompra boolean, numerocartaoexportacaounico boolean, codigoexportacao varchar, tabelarestricoestrocaid bigint, usuariowebconvenios varchar, senhawebconvenios varchar, tipocalculolimite varchar, cadernoofertaid bigint, codigoimportacao varchar, bloquearbuscamanual boolean, integracaocontabilcodigo varchar, emitecartaointerno boolean, comprovanteimprimirsaldorestante boolean, layoutexportacaoarquivoid bigint, plataformatef varchar,planoid VARCHAR)
)SELECT 
   'select nq(''insert into crediario(id,status,nome,limitepadraocliente,planopagamentoid,taxajuros,taxamulta,taxaconvenio,toleranciaatraso,tipo,recebimentominimo,autorizacao,toleranciajuros,toleranciamulta,layoutexportacao,layoutimportacaocliente,comprovanteimprimirviaparcela,comprovanteimprimiritemvenda,desconsiderarcompra,numerocartaoexportacaounico,tipocalculolimite,bloquearbuscamanual,emitecartaointerno,comprovanteimprimirsaldorestante) 
   values('||generate_id()||','''''||STATUS||''''','''''||crediario_brava.nome||''''','||limitepadraocliente||','||planopagamento.id||','||taxajuros||','||taxamulta||','||taxaconvenio||','||toleranciaatraso||','''''||tipo||''''','||recebimentominimo||','''''||autorizacao||''''','||toleranciajuros||','||toleranciamulta||','''''||layoutexportacao||''''','''''||layoutimportacaocliente||''''','||comprovanteimprimirviaparcela||','||comprovanteimprimiritemvenda||','||desconsiderarcompra||','||numerocartaoexportacaounico||','''''||tipocalculolimite||''''','||bloquearbuscamanual||','||emitecartaointerno||','||comprovanteimprimirsaldorestante||')'');'
FROM crediario_brava
JOIN planopagamento ON planopagamento.nome = crediario_brava.planoid
"""

cadernooferta_brava = """
-- INSERT CADERNO DE OFERTA
WITH cadernooferta_brava AS (
SELECT
  *
FROM dblink('conexao_baseorigem', '
SELECT 
	*
FROM 
  cadernooferta 
WHERE 
  id in ({ids})
') AS DATA (id bigint, status varchar, nome varchar, datahorainicial timestamp, datahorafinal timestamp, origem varchar, codigointegracao varchar, intervalotempo varchar, diasemanasegunda boolean, diasemanaterca boolean, diasemanaquarta boolean, diasemanaquinta boolean, diasemanasexta boolean, diasemanasabado boolean, diasemanadomingo boolean, diasemanaordinalnumero varchar, diasemanaordinaldiasemana varchar, diasmes varchar, crmrequerpessoavenda boolean, crmrequercpfcnpj boolean, crmrequercelular boolean, crmrequeremail boolean, crmrequercelulartelefone boolean, crmrequerdatanascimento boolean, crmrequerendereco boolean, crmapenasprimeiravendapessoa varchar, crmapenasentrega boolean, formapagamentoaceita varchar, restringedescontomanual boolean, restringedemaisofertas boolean, crmapenasgrupoconsumidor boolean, validoapenasprevencido boolean, diasparavencimento integer, validoapenascupomdesconto boolean)
)
SELECT
'select nq (''insert into cadernooferta (id,status,nome,origem,intervalotempo,diasemanasegunda,diasemanaterca,diasemanaquarta,diasemanaquinta,diasemanasexta,diasemanasabado,diasemanadomingo,diasemanaordinalnumero,diasemanaordinaldiasemana,crmrequerpessoavenda,crmrequercpfcnpj,crmrequercelular,crmrequeremail,crmrequercelulartelefone,crmrequerdatanascimento,crmrequerendereco,crmapenasprimeiravendapessoa,crmapenasentrega,formapagamentoaceita,restringedescontomanual,restringedemaisofertas,crmapenasgrupoconsumidor,validoapenasprevencido)
values('||generate_id()||','''''||STATUS||''''','''''||nome||''''','''''||origem||''''','''''||intervalotempo||''''','||diasemanasegunda||','||diasemanaterca||','||diasemanaquarta||','||diasemanaquinta||','||diasemanasexta||','||diasemanasabado||','||diasemanadomingo||','''''||diasemanaordinalnumero||''''',
'''''||diasemanaordinaldiasemana||''''','||crmrequerpessoavenda||','||crmrequercpfcnpj||','||crmrequercelular||','||crmrequeremail||','||crmrequercelulartelefone||','||crmrequerdatanascimento||','||crmrequerendereco||','''''||crmapenasprimeiravendapessoa||''''','||crmapenasentrega||','''''||formapagamentoaceita||''''','||restringedescontomanual||','||restringedemaisofertas||','||crmapenasgrupoconsumidor||','||validoapenasprevencido||')'');'
FROM cadernooferta_brava
"""

cadernooferta_brava_2 = """
-- INSERT CADERNO DE OFERTA
WITH cadernooferta_brava AS (
SELECT
  *
FROM dblink('conexao_baseorigem', '
SELECT 
	*
FROM 
  cadernooferta 
WHERE 
  id in ({ids}) 
') AS DATA (id bigint, status varchar, nome varchar, datahorainicial timestamp, datahorafinal timestamp, origem varchar, codigointegracao varchar, intervalotempo varchar, diasemanasegunda boolean, diasemanaterca boolean, diasemanaquarta boolean, diasemanaquinta boolean, diasemanasexta boolean, diasemanasabado boolean, diasemanadomingo boolean, diasemanaordinalnumero varchar, diasemanaordinaldiasemana varchar, diasmes varchar, crmrequerpessoavenda boolean, crmrequercpfcnpj boolean, crmrequercelular boolean, crmrequeremail boolean, crmrequercelulartelefone boolean, crmrequerdatanascimento boolean, crmrequerendereco boolean, crmapenasprimeiravendapessoa varchar, crmapenasentrega boolean, formapagamentoaceita varchar, restringedescontomanual boolean, restringedemaisofertas boolean, crmapenasgrupoconsumidor boolean, validoapenasprevencido boolean, diasparavencimento integer, validoapenascupomdesconto boolean)
)
SELECT
'select nq (''insert into cadernooferta (id,status,nome,origem,intervalotempo,diasemanasegunda,diasemanaterca,diasemanaquarta,diasemanaquinta,diasemanasexta,diasemanasabado,diasemanadomingo,diasemanaordinalnumero,diasemanaordinaldiasemana,crmrequerpessoavenda,crmrequercpfcnpj,crmrequercelular,crmrequeremail,crmrequercelulartelefone,crmrequerdatanascimento,crmrequerendereco,crmapenasprimeiravendapessoa,crmapenasentrega,formapagamentoaceita,restringedescontomanual,restringedemaisofertas,crmapenasgrupoconsumidor,validoapenasprevencido)
values('||generate_id()||','''''||STATUS||''''','''''||nome||''''','''''||origem||''''','''''||intervalotempo||''''','||diasemanasegunda||','||diasemanaterca||','||diasemanaquarta||','||diasemanaquinta||','||diasemanasexta||','||diasemanasabado||','||diasemanadomingo||','''''||diasemanaordinalnumero||''''',
'''''||diasemanaordinaldiasemana||''''','||crmrequerpessoavenda||','||crmrequercpfcnpj||','||crmrequercelular||','||crmrequeremail||','||crmrequercelulartelefone||','||crmrequerdatanascimento||','||crmrequerendereco||','''''||crmapenasprimeiravendapessoa||''''','||crmapenasentrega||','''''||formapagamentoaceita||''''','||restringedescontomanual||','||restringedemaisofertas||','||crmapenasgrupoconsumidor||','||validoapenasprevencido||')'');'
FROM cadernooferta_brava
"""

cadernooferta_brava_classificacao= """
--CLASSIFICACAO
WITH itemcaderno_brava AS (
SELECT
  *
FROM dblink('conexao_baseorigem', '
SELECT 
  itemcadernooferta.*,
  classificacao.caminho,
  cadernooferta.nome as cadernonome
FROM 
  itemcadernooferta
  JOIN classificacao on classificacao.id = itemcadernooferta.classificacaoid
  JOIN cadernooferta on cadernooferta.id = itemcadernooferta.cadernoofertaid
WHERE
  itemcadernooferta.cadernoofertaid in ({ids}) --ID DO CADERNO DE OFERTA ESCOLHIDO PELO CLIENTE 
') AS DATA (id bigint, embalagemid bigint, cadernoofertaid bigint, tipooferta varchar, precooferta numeric (15,4), descontooferta numeric (15,4), leve integer, pague integer, descontolevepague numeric (15,4), tipoitem varchar, gruporemarcacaoid bigint, fabricanteid bigint, classificacaoid bigint, markup numeric (15,4), descontoporqtdtipo varchar, descontoporqtdvendaacimaqtd varchar, comboofertaid bigint, comboquantidadevenda integer, cadernoofertafaixaparcelamentoid bigint, ultimaedicaodatahora timestamp, ultimaedicaousuarioid bigint, quantidadeutilizacaoofertavenda integer, grupoembalagemid bigint, idscanntech bigint, tituloscanntech varchar,caminho VARCHAR,cadernonome VARCHAR)
)
SELECT
'select nq (''insert into itemcadernooferta (id,cadernoofertaid,tipooferta,descontooferta,tipoitem,classificacaoid,descontoporqtdtipo,descontoporqtdvendaacimaqtd)
values('||generate_id()||','||cadernooferta.id||','''''||tipooferta||''''','||descontooferta||','''''||tipoitem||''''','||classificacao.id||','''''||descontoporqtdtipo||''''','''''||descontoporqtdvendaacimaqtd||''''')'');'
FROM itemcaderno_brava
JOIN classificacao ON classificacao.caminho = itemcaderno_brava.caminho
JOIN cadernooferta ON cadernooferta.nome = itemcaderno_brava.cadernonome
"""

cadernooferta_brava_classificacao_2 = """
--CLASSIFICACAO
WITH itemcaderno_brava AS (
SELECT
  *
FROM dblink('conexao_baseorigem', '
SELECT 
  itemcadernooferta.*,
  classificacao.caminho,
  cadernooferta.nome as cadernonome
FROM 
  itemcadernooferta
  JOIN classificacao on classificacao.id = itemcadernooferta.classificacaoid
  JOIN cadernooferta on cadernooferta.id = itemcadernooferta.cadernoofertaid
WHERE
  itemcadernooferta.cadernoofertaid in ({ids}) --ID DO CADERNO DE OFERTA ESCOLHIDO PELO CLIENTE 
') AS DATA (id bigint, embalagemid bigint, cadernoofertaid bigint, tipooferta varchar, precooferta numeric (15,4), descontooferta numeric (15,4), leve integer, pague integer, descontolevepague numeric (15,4), tipoitem varchar, gruporemarcacaoid bigint, fabricanteid bigint, classificacaoid bigint, markup numeric (15,4), descontoporqtdtipo varchar, descontoporqtdvendaacimaqtd varchar, comboofertaid bigint, comboquantidadevenda integer, cadernoofertafaixaparcelamentoid bigint, ultimaedicaodatahora timestamp, ultimaedicaousuarioid bigint, quantidadeutilizacaoofertavenda integer, grupoembalagemid bigint, idscanntech bigint, tituloscanntech varchar,caminho VARCHAR,cadernonome VARCHAR)
)
SELECT
'select nq (''insert into itemcadernooferta (id,cadernoofertaid,tipooferta,descontooferta,tipoitem,classificacaoid,descontoporqtdtipo,descontoporqtdvendaacimaqtd)
values('||generate_id()||','||cadernooferta.id||','''''||tipooferta||''''','||descontooferta||','''''||tipoitem||''''','||classificacao.id||','''''||descontoporqtdtipo||''''','''''||descontoporqtdvendaacimaqtd||''''')'');'
FROM itemcaderno_brava
JOIN classificacao ON classificacao.caminho = itemcaderno_brava.caminho
JOIN cadernooferta ON cadernooferta.nome = itemcaderno_brava.cadernonome
"""

curvaabc_brava = """
WITH curva_brava AS (
SELECT 
*
FROM dblink('conexao_baseorigem',
'SELECT
	*
FROM curvaabc;'
) AS DATA (id BIGINT, nome VARCHAR, porcentagem INTEGER, intervaloinicial INTEGER, intervalofinal INTEGER)
)SELECT
	'select nq(''update curvaabc set porcentagem = '||curva_brava.porcentagem||', intervaloinicial = '||curva_brava.intervaloinicial||', intervalofinal = '||curva_brava.intervalofinal||' where id = '||curvaabc.id||''');'
FROM curva_brava 
JOIN curvaabc ON curvaabc.id = curva_brava.id
"""

fidelidade_brava = """
-- FIDELIDADE
WITH fidelidade_brava AS (
SELECT
  *
FROM dblink('conexao_baseorigem',
'select 
	id, 
	status, 
	nome, 
	tipo, 
	validadetipo,
	validadediasresgate,
	regrapbm, regracrediario,
	fidelizaporcredito, 
	fidelizaporponto, 
	fidelizaporquantidade, 
	regragrupoconsumidores 
from fidelidade
where status = ''A''
') AS DATA (id BIGINT, STATUS VARCHAR, nome VARCHAR, tipo VARCHAR, validadetipo VARCHAR, validadediasresgate INT, regrapbm VARCHAR, regracrediario VARCHAR, 
fidelizaporcredito BOOLEAN, fidelizaporponto BOOLEAN, fidelizaporquantidade BOOLEAN, regragrupoconsumidores VARCHAR)
)
SELECT
	'select nq(''insert into fidelidade (id, status, nome, tipo, validadetipo, validadediasresgate, regrapbm, regracrediario, fidelizaporcredito, fidelizaporponto, fidelizaporquantidade, regragrupoconsumidores) values('||generate_id()||', '''''||STATUS||''''', '''''||nome||''''', '''''||tipo||''''', '''''||validadetipo||''''', '||validadediasresgate||', '''''||regrapbm||''''', '''''||regracrediario||''''', '||fidelizaporcredito||', '||fidelizaporponto||', '||fidelizaporquantidade||', '''''||regragrupoconsumidores||''''')'');'
FROM fidelidade_brava;
"""

fidelidade_classificacao_brava = """
-- CLASSIFICACAO FIDELIDADE 
WITH fidelidade_brava AS (
SELECT
  *
FROM dblink('conexao_baseorigem',
'select 
	id, 
	status, 
	nome, 
	tipo, 
	validadetipo,
	validadediasresgate,
	regrapbm, regracrediario,
	fidelizaporcredito, 
	fidelizaporponto, 
	fidelizaporquantidade, 
	regragrupoconsumidores 
from fidelidade
where status = ''A''
') AS DATA (id BIGINT, STATUS VARCHAR, nome VARCHAR, tipo VARCHAR, validadetipo VARCHAR, validadediasresgate INT, regrapbm VARCHAR, regracrediario VARCHAR, 
fidelizaporcredito BOOLEAN, fidelizaporponto BOOLEAN, fidelizaporquantidade BOOLEAN, regragrupoconsumidores VARCHAR)
), classificacaofidelidade_brava AS (
SELECT 
*
FROM dblink('conexao_baseorigem',
'SELECT 
	classificacao.nome,
	classificacao.caminho,
	classificacao.id AS classificacaoidd,
	fidelidadeclassificacao.* 
FROM fidelidadeclassificacao 
JOIN classificacao ON classificacao.id = fidelidadeclassificacao.classificacaoid
') AS DATA (nome VARCHAR, caminho VARCHAR, classificacaoidd BIGINT,id BIGINT, fidelidadeid BIGINT,classificacaoid BIGINT, credito NUMERIC,moedaporponto NUMERIC,
quantidadeporponto NUMERIC,crediarioid BIGINT,pbmid BIGINT, grupoconsumidoresid BIGINT,tipofidelidade VARCHAR)
)
SELECT
	'select nq(''insert into fidelidadeclassificacao (id, fidelidadeid, classificacaoid, moedaporponto, tipofidelidade) values ('||generate_id()||', '||fidelidade.id||', '||classificacao.id||', '||classificacaofidelidade_brava.moedaporponto||', '''''||classificacaofidelidade_brava.tipofidelidade||''''')'');'
FROM classificacaofidelidade_brava 
JOIN fidelidade_brava ON fidelidade_brava.id = classificacaofidelidade_brava.fidelidadeid
JOIN fidelidade ON fidelidade.nome = fidelidade_brava.nome
--JOIN classificacao ON classificacao.caminho = SPLIT_PART(classificacaofidelidade_brava.caminho, '>', 1)||'> PADRÃO BRAVA >'||SUBSTRING(classificacaofidelidade_brava.caminho, 12, 200); --Verificar se é importacao ou base limpa
JOIN classificacao ON classificacao.caminho = classificacaofidelidade_brava.caminho;
"""

fidelidade_pontos_brava = """
--PREMIOS E PONTOS FIDELIDADE 
WITH premiosfidelidade_brava AS (
SELECT 
*
FROM dblink('conexao_baseorigem',
'select 
	embalagem.codigobarras,
	fidelidadepremio.pontos,
	fidelidade.nome
from fidelidadepremio 
join embalagem on embalagem.id = fidelidadepremio.embalagemid
join fidelidade on fidelidade.id = fidelidadepremio.fidelidadeid
') AS DATA (codigobarras VARCHAR,pontos NUMERIC,nome VARCHAR)
), insertpontos AS (
SELECT 
	embalagem.id AS id,
	premiosfidelidade_brava.pontos::NUMERIC AS pontos,
	fidelidade.id AS idfidelidade
FROM premiosfidelidade_brava
JOIN fidelidade ON fidelidade.nome = premiosfidelidade_brava.nome
JOIN embalagem ON embalagem.codigobarras = premiosfidelidade_brava.codigobarras
) 
SELECT 
'select nq(''insert into fidelidadepremio (id, fidelidadeid, embalagemid, pontos) values ('||generate_id()||','||idfidelidade||','||id||','||pontos||')'');'
FROM insertpontos;
"""

planodeconta_delete = """
-- Fazendo BACKUP
SELECT
	*
INTO bkp_planocontas2
FROM planocontas
ORDER BY profundidade;
 
SELECT
	'select nq(''delete from planocontas where id = '||id||''');'
FROM planocontas
ORDER BY profundidade DESC;
"""

planodeconta_1 = """
-- PROFUNDIDADE 1
WITH planoconta_brava AS (
SELECT * FROM dblink('conexao_baseorigem', '
SELECT 
	id , nome , folha , profundidade , planocontaspaiid , caminho , integracaocontabilcodigo , grupoafericaoindicadores 
FROM planocontas
ORDER BY profundidade;'
) AS DATA (id BIGINT, nome VARCHAR, folha BOOLEAN, profundidade INTEGER, planocontaspaiid BIGINT, caminho VARCHAR, 
integracaocontabilcodigo VARCHAR, grupoafericaoindicadores VARCHAR)
)SELECT
	'select nq(''INSERT INTO planocontas (id, nome, folha, profundidade, caminho, integracaocontabilcodigo, grupoafericaoindicadores) VALUES('||generate_id()||', '''''||REPLACE(nome,'''','')||''''', '||folha||', '||profundidade||', '''''||REPLACE(caminho,'''','')||''''', '''''||COALESCE(integracaocontabilcodigo, 'NULL')||''''', '''''||grupoafericaoindicadores||''''')'');'
FROM planoconta_brava 
WHERE profundidade = 1;
"""

planodeconta_2 = """
-- PROFUNDIDADE 2
WITH planoconta_brava AS (
SELECT * FROM dblink('conexao_baseorigem', '
SELECT 
	id , nome , folha , profundidade , planocontaspaiid , caminho , integracaocontabilcodigo , grupoafericaoindicadores 
FROM planocontas
ORDER BY profundidade;'
) AS DATA (id BIGINT, nome VARCHAR, folha BOOLEAN, profundidade INTEGER, planocontaspaiid BIGINT, caminho VARCHAR, 
integracaocontabilcodigo VARCHAR, grupoafericaoindicadores VARCHAR)
)SELECT 
	'select nq(''INSERT INTO planocontas (id, nome, folha, profundidade, planocontaspaiid, caminho, integracaocontabilcodigo, grupoafericaoindicadores) VALUES('||generate_id()||', '''''||REPLACE(planoconta_brava.nome,'''','')||''''', '||planoconta_brava.folha||', '||planoconta_brava.profundidade||', '||planocontas.id||', '''''||REPLACE(planoconta_brava.caminho,'''','')||''''', '''''||COALESCE(planoconta_brava.integracaocontabilcodigo, 'NULL')||''''', '''''||planoconta_brava.grupoafericaoindicadores||''''')'');'
FROM planoconta_brava 
JOIN planoconta_brava plan ON plan.id = planoconta_brava.planocontaspaiid
JOIN planocontas ON planocontas.nome = plan.nome
WHERE planoconta_brava.profundidade = 2;
"""

planodeconta_3 = """
-- PROFUNDIDADE 3
WITH planoconta_brava AS (
SELECT * FROM dblink('conexao_baseorigem', '
SELECT 
	id , nome , folha , profundidade , planocontaspaiid , caminho , integracaocontabilcodigo , grupoafericaoindicadores 
FROM planocontas
ORDER BY profundidade;'
) AS DATA (id BIGINT, nome VARCHAR, folha BOOLEAN, profundidade INTEGER, planocontaspaiid BIGINT, caminho VARCHAR, 
integracaocontabilcodigo VARCHAR, grupoafericaoindicadores VARCHAR)
)SELECT 
	'select nq(''INSERT INTO planocontas (id, nome, folha, profundidade, planocontaspaiid, caminho, integracaocontabilcodigo, grupoafericaoindicadores) VALUES('||generate_id()||', '''''||REPLACE(planoconta_brava.nome,'''','')||''''', '||planoconta_brava.folha||', '||planoconta_brava.profundidade||', '||planocontas.id||', '''''||REPLACE(planoconta_brava.caminho,'''','')||''''', '''''||COALESCE(planoconta_brava.integracaocontabilcodigo, 'NULL')||''''', '''''||planoconta_brava.grupoafericaoindicadores||''''')'');'
FROM planoconta_brava 
JOIN planoconta_brava plan ON plan.id = planoconta_brava.planocontaspaiid
JOIN planocontas ON planocontas.nome = plan.nome
WHERE planoconta_brava.profundidade = 3;
"""

planodeconta_4 = """
-- PROFUNDIDADE 4
WITH planoconta_brava AS (
SELECT * FROM dblink('conexao_baseorigem', '
SELECT 
	id , nome , folha , profundidade , planocontaspaiid , caminho , integracaocontabilcodigo , grupoafericaoindicadores 
FROM planocontas
ORDER BY profundidade;'
) AS DATA (id BIGINT, nome VARCHAR, folha BOOLEAN, profundidade INTEGER, planocontaspaiid BIGINT, caminho VARCHAR, 
integracaocontabilcodigo VARCHAR, grupoafericaoindicadores VARCHAR)
)SELECT 
	'select nq(''INSERT INTO planocontas (id, nome, folha, profundidade, planocontaspaiid, caminho, integracaocontabilcodigo, grupoafericaoindicadores) VALUES('||generate_id()||', '''''||REPLACE(planoconta_brava.nome,'''','')||''''', '||planoconta_brava.folha||', '||planoconta_brava.profundidade||', '||planocontas.id||', '''''||REPLACE(planoconta_brava.caminho,'''','')||''''', '''''||COALESCE(planoconta_brava.integracaocontabilcodigo, 'NULL')||''''', '''''||planoconta_brava.grupoafericaoindicadores||''''')'');'
FROM planoconta_brava 
JOIN planoconta_brava plan ON plan.id = planoconta_brava.planocontaspaiid
JOIN planocontas ON planocontas.nome = plan.nome
WHERE planoconta_brava.profundidade = 4;
"""

planodeconta_5 = """
-- PROFUNDIDADE 5
WITH planoconta_brava AS (
SELECT * FROM dblink('conexao_baseorigem', '
SELECT 
	id , nome , folha , profundidade , planocontaspaiid , caminho , integracaocontabilcodigo , grupoafericaoindicadores 
FROM planocontas
ORDER BY profundidade;'
) AS DATA (id BIGINT, nome VARCHAR, folha BOOLEAN, profundidade INTEGER, planocontaspaiid BIGINT, caminho VARCHAR, 
integracaocontabilcodigo VARCHAR, grupoafericaoindicadores VARCHAR)
)SELECT 
	'select nq(''INSERT INTO planocontas (id, nome, folha, profundidade, planocontaspaiid, caminho, integracaocontabilcodigo, grupoafericaoindicadores) VALUES('||generate_id()||', '''''||REPLACE(planoconta_brava.nome,'''','')||''''', '||planoconta_brava.folha||', '||planoconta_brava.profundidade||', '||planocontas.id||', '''''||REPLACE(planoconta_brava.caminho,'''','')||''''', '''''||COALESCE(planoconta_brava.integracaocontabilcodigo, 'NULL')||''''', '''''||planoconta_brava.grupoafericaoindicadores||''''')'');'
FROM planoconta_brava 
JOIN planoconta_brava plan ON plan.id = planoconta_brava.planocontaspaiid
JOIN planocontas ON planocontas.nome = plan.nome
WHERE planoconta_brava.profundidade = 5;
"""

planodeconta_final = """
SELECT
	'select nq(''update planocontas set integracaocontabilcodigo = null where id = '||id||''');' 
FROM planocontas 
WHERE integracaocontabilcodigo = 'NULL';
"""

comissao_bonificacao = """
-- PLANO DE REMUNERAÇÃO 
WITH planoremuneracao_brava AS (
SELECT * FROM dblink('conexao_baseorigem', '
SELECT 
	*
FROM planoremuneracao
WHERE status = ''A'''
) AS DATA (id BIGINT, STATUS VARCHAR, nome VARCHAR, datainicial TIMESTAMP, datafinal TIMESTAMP, remunerardevolucaoestorno BOOLEAN, tipo VARCHAR, 
validoapenasprevencido BOOLEAN, diasparavencimento INTEGER, remuneraritemautorizadopbm BOOLEAN)
)SELECT 
'select nq (''insert into planoremuneracao (id, status, nome, datainicial, datafinal, remunerardevolucaoestorno, tipo)
values ('||generate_id()||','''''||STATUS||''''','''''||nome||''''','''''||datainicial||''''','''''||datafinal||''''','||remunerardevolucaoestorno||','''''||tipo||''''')'');' 
FROM planoremuneracao_brava;
"""

bonificacao_item = """
-- ITENS BONIFICAÇAO 
WITH bonificacao_brava AS (
SELECT * FROM dblink('conexao_baseorigem', '
SELECT 
	planoremuneracao.nome,
	embalagem.codigobarras,
	pb.bonificacao
FROM planoremuneracaobonificacaoembalagem pb
JOIN embalagem on embalagem.id = pb.embalagemid
JOIN planoremuneracao on planoremuneracao.id = pb.planoremuneracaoid
') AS DATA (nome VARCHAR, codigobarras VARCHAR, bonificacao NUMERIC(15,4))
)SELECT 
'select nq (''insert into planoremuneracaobonificacaoembalagem (id, planoremuneracaoid,  embalagemid, bonificacao)
values('||generate_id()||','||planoremuneracao.id||','||embalagem.id||','||bonificacao_brava.bonificacao||')'');'
FROM bonificacao_brava
JOIN planoremuneracao ON planoremuneracao.nome = bonificacao_brava.nome
JOIN embalagem ON embalagem.codigobarras = bonificacao_brava.codigobarras;
"""

classificacao_comissao = """
-- CLASSIFICACAO REMUNERACAO
WITH plano_classificacao AS (
SELECT * FROM dblink('conexao_baseorigem', '
SELECT 
	planoremuneracao.nome as plano,
	classificacao.nome as nome_class,
	classificacao.caminho,
	pc.comissao
FROM planoremuneracaocomissaoclassificacao pc
JOIN planoremuneracao on planoremuneracao.id = pc.planoremuneracaoid
JOIN classificacao on classificacao.id = pc.classificacaoid
') AS DATA (plano VARCHAR, nome_class VARCHAR, caminho VARCHAR, comissao NUMERIC(15,4))
)SELECT 
'select nq (''insert into planoremuneracaocomissaoclassificacao (id, planoremuneracaoid,  classificacaoid, comissao)
values('||generate_id()||','||planoremuneracao.id||','||classificacao.id||','||plano_classificacao.comissao||')'');'
FROM plano_classificacao
JOIN planoremuneracao ON planoremuneracao.nome = plano_classificacao.plano
--JOIN classificacao ON classificacao.caminho = SPLIT_PART(plano_classificacao.caminho, '>', 1)||'> PADRÃO BRAVA >'||SUBSTRING(plano_classificacao.caminho, 12, 200);--VERIFICAR IMPORTAÇÃO OU BASE LIMPA
JOIN classificacao on classificacao.caminho = plano_classificacao.caminho
"""

item_comissao = """
-- ITENS COMISSAO 
WITH comissao_brava AS (
SELECT * FROM dblink('conexao_baseorigem', '
SELECT 
	planoremuneracao.nome,
	embalagem.codigobarras,
	pb.comissao
FROM planoremuneracaocomissaoembalagem pb
JOIN embalagem on embalagem.id = pb.embalagemid
JOIN planoremuneracao on planoremuneracao.id = pb.planoremuneracaoid
WHERE planoremuneracao.status = ''A''
') AS DATA (nome VARCHAR, codigobarras VARCHAR, comissao NUMERIC(15,4))
)SELECT 
'select nq (''insert into planoremuneracaocomissaoembalagem (id, planoremuneracaoid,  embalagemid, comissao)
values('||generate_id()||','||planoremuneracao.id||','||embalagem.id||','||comissao_brava.comissao||')'');'
FROM comissao_brava
JOIN planoremuneracao ON planoremuneracao.nome = comissao_brava.nome
JOIN embalagem ON embalagem.codigobarras = comissao_brava.codigobarras;
"""

dias_estocagem = """
WITH diasestocagem_brava AS (
SELECT 
*
FROM dblink('conexao_baseorigem',
'select 
	classificacaocurvaabcunidadenegocio.id , classificacaocurvaabcunidadenegocio.curvaabcid ,classificacaocurvaabcunidadenegocio.classificacaoid ,
	classificacaocurvaabcunidadenegocio.unidadenegocioid ,classificacaocurvaabcunidadenegocio.diasestocagem ,
	classificacao.nome,
	classificacao.caminho 
from classificacaocurvaabcunidadenegocio
join classificacao on classificacao.id = classificacaocurvaabcunidadenegocio.classificacaoid
where unidadenegocioid = %s'
) AS DATA (id BIGINT, curvaabcid BIGINT,classificacaoid BIGINT,unidadenegocioid BIGINT,diasestocagem NUMERIC,nome VARCHAR, caminho VARCHAR)
),
idclassificacao AS (
SELECT
	db.curvaabcid,
    c.id AS classificacaoid,
	db.diasestocagem
FROM classificacao c
JOIN diasestocagem_brava db ON c.caminho = db.caminho
)
SELECT 
    'select nq(''insert into classificacaocurvaabcunidadenegocio values ('||generate_id()||','||curvaabcid||','||classificacaoid||',1,'||diasestocagem||')'');' --SETAR O ID DA LOJA
FROM idclassificacao;
"""

demanda_brava = """
-- DEMANDA 
WITH demanda_brava AS (
SELECT * FROM dblink('conexao_baseorigem', '
 select 
   embalagem.codigobarras,
   historicovenda.data,
   historicovenda.quantidade
from historicovenda
join embalagem on embalagem.id = historicovenda.embalagemid
where historicovenda.unidadenegocioid =  %s  
 and historicovenda.data >= now() - interval ''1'' month
 and embalagem.codigobarras is not null;
') AS DATA (codigobarras VARCHAR, DATA VARCHAR, quantidade NUMERIC(15,4))
)SELECT 
   'select nq(''insert into historicovenda(id,unidadenegocioid,embalagemid,data,quantidade) values('||generate_id()||',1,'||embalagem.id||','''''||DATA||''''','||quantidade||')'');'
FROM demanda_brava
JOIN embalagem ON embalagem.codigobarras = demanda_brava.codigobarras;
"""




