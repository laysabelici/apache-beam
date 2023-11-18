#IMPORT BIBLIOTECA
import apache_beam as beam

p1 = beam.Pipeline()

Tempo_Atrasos = (
    p1
    |"Importar dados Tempo" >> beam.io.ReadFromText(r'C:\\Users\\laybe\\OneDrive\\Área de Trabalho\\apache-beam\dados\\voos_sample.csv', skip_header_lines = 1)
    |"Separar por vírgula Tempo" >> beam.Map(lambda record: record.split(','))
    #Realiza uma separação estilo booleano, também foi indicado a coluna para a ação.
    #Agora vamos pegar todos os itens maior que 0 da coluna 8, referentes a atrasos.
    #temmos que indicar o tipo de dado da coluna, pois ele trata sempre como str.
    |"Pegar voos atrasados Tempo" >> beam.Filter(lambda record: int(record[8]) > 0)
    #Criando uma Key e um Value, utilizando a coluna 4 (destino) e o atraso.
    |"Criar conjunto par Tempo" >> beam.Map(lambda record: (record[4], int(record[8])))
    # Soma todas as keys que são iguais, para um value unico
    |"Somar por key Tempo" >> beam.CombinePerKey(sum)
)

Qtd_Atrasos = (
    p1
    |"Importar dados QTD" >> beam.io.ReadFromText(r'C:\\Users\\laybe\\OneDrive\\Área de Trabalho\\apache-beam\dados\\voos_sample.csv', skip_header_lines = 1)
    |"Separar por vírgula QTD" >> beam.Map(lambda record: record.split(','))
    #Realiza uma separação estilo booleano, também foi indicado a coluna para a ação.
    #Agora vamos pegar todos os itens maior que 0 da coluna 8, referentes a atrasos.
    #temmos que indicar o tipo de dado da coluna, pois ele trata sempre como str.
    |"Pegar voos atrasados QTD" >> beam.Filter(lambda record: int(record[8]) > 0)
    #Criando uma Key e um Value, utilizando a coluna 4 (destino) e o atraso.
    |"Criar conjunto par QTD" >> beam.Map(lambda record: (record[4], int(record[8])))
    # Soma todas as keys que são iguais, para um value unico
    #Fornece a quantidade de atrasos por aeroporto
    |"Somar por key QTD" >> beam.combiners.Count.PerKey(sum)
)

tabela_atrasos = (
    #Criando tabela sendo dicionário.
    #Essa estrutura pode ser útil em determinados casos,
    #especialmente se você estiver agrupando ou processando dados
    #que têm uma relação específica entre eles.
    {'Qtd_Atrasos':Qtd_Atrasos, 'Tempo_Atrasos':Tempo_Atrasos}
    #É usada para agrupar esses dados por chave.
    #útil quando precisamos realizar operações de agregação ou processamento
    #em grupos específicos de dados.
    |"Group by" >> beam.CoGroupByKey()
    |"Mostrar dados" >> beam.Map(print)
)

p1.run()
