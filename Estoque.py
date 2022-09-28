import warnings

import yaml


class Estoque:
    def __init__(self):
        self.db = {}
        self.tipos_produtos = []

        print('Gerando estoque vazio...')

        with open('produtos.yaml', 'r') as f:
            produtos = yaml.load(f, Loader=yaml.FullLoader)

            for p in produtos:
                self.db[p['pid']] = {'classe': p['classe'], 'pid': p['pid'], 'qntd': 0}

        self.tipos_produtos = list(self.db.keys())
        print('Estoque carregado')

    def credito(self, pid, qntd) -> int:
        if pid not in self.db:
            raise Exception(f'Credito em PID ({pid}) desconhecido')

        if qntd <= 0:
            raise Exception(f'Credito negativo em {pid}: {qntd}')

        self.db[pid]['qntd'] += qntd

        return self.db[pid]['qntd']

    def debito(self, pid, qntd) -> bool:
        if pid not in self.db:
            raise Exception(f'Debito em PID ({pid}) desconhecido')

        if qntd <= 0:
            raise Exception(f'Debito negativo em {pid}: {qntd}')

        if self.db[pid]['qntd'] < qntd:
            warnings.warn(f'Falha em debito em {pid}: disponivel {self.db[pid]["qntd"]} debito {qntd}')
            return False

        self.db[pid]['qntd'] -= qntd

        return self.db[pid]['qntd']
