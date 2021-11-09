# Desafio modulo 3 Cloud Data Engineer IGTI

Desafio feito utilizando os serviços do Dataproc e Storage da Google Cloud Plataform

Objetivos:

* Construir o schema seguindo o [Dicionario](https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/arquivos/NOVOLAYOUTDOSDADOSABERTOSDOCNPJ.pdf)
* Ler as tabelas disponibilizadas em um bucket publico do GCS
* Escrever as tabelas individuais em um bucket privado do GCS chamado "trusted".
* Realizar operações subjetivas nas tabelas da pasta trusted, de forma a tornar as
tabelas mais interessantes e confiáveis para o usuário final.
* Juntar as tabelas, consolidando as informações em um único DataFrame.
* Escrever os dados em um do bucket privado do GCS, chamada "refined".
* Analisar os dados respondendo as perguntas do desafio.