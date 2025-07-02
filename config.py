projetos = ['trilhasamazonia', 'trilhasamazonia2']

aplicativos = [
    'faturamento'
    # , 'financeiro'
    # , 'compras'
]

categorias_por_aplicativo = {
    "faturamento": [
        'OrdemVenda'
        , 'OrdemVendaProduto'
        , 'Produto'
        # 'notaFiscal'
        , 'NFeProduto'
        , 'categoria'
        , 'pessoa'
        , 'tabelaPreco'
    ],
    "financeiro": [
        'centroCusto',
        'contaPagar',  
        'contaReceber'
    ],
    "compras": [
        'pedido'
    ]
}