from datetime import datetime

import yaml

import util


class Estoque:
    def __init__(self, centro=False, log=False):
        # "Database" -> dict no formato <PID> : {"pid": <PID>, "classe": <CLASSE>, "qntd": <QUANTIDADE>}
        self.db = {}
        
        # Lista com todos os pids
        self.tipos_produtos = []
        
        # Registrar operações de crédito/débito
        # Usado pelo visualizador
        self.log = log
        self.log_data = []

        print('Gerando estoque cheio...')

        with open('produtos.yaml', 'r') as f:
            produtos = yaml.load(f, Loader=yaml.FullLoader)

            for p in produtos:
                qntd = util.max_estoque(p['classe'], centro=centro)

                self.db[p['pid']] = {'classe': p['classe'], 'pid': p['pid'], 'qntd': qntd}

        self.tipos_produtos = list(self.db.keys())
        print('Estoque carregado')

    # Se o produto de determinado pid está abaixo de 25% (cor vermelha)
    def precisa_reestoque(self, pid) -> int:
        classe = self.db[pid]['classe']
        curr_estoque = self.db[pid]['qntd']

        cor = util.cor_estoque(classe, curr_estoque)

        # print(f'[{pid}] {cor}: {curr_estoque}/{util.max_estoque(classe)}')

        if cor == 'red':
            return util.max_estoque(classe) - curr_estoque

        return 0

    # Operação de crédito de determinada quantidade em determinado PID
    # Retorna a nova quantidade disponível
    # Levanta exceptions quando o PID é desconhecido ou a qntd é negativa
    def credito(self, pid, qntd) -> int:
        if pid not in self.db:
            raise Exception(f'Credito em PID ({pid}) desconhecido')

        if qntd <= 0:
            raise Exception(f'Credito negativo em {pid}: {qntd}')

        self.db[pid]['qntd'] += qntd

        if self.log:
            wrapped_text = util.wrap_color('#228B22', f'{pid}: +{qntd}')
            self.log_data.insert(0, f'<div>[{datetime.now()}] </div>{wrapped_text}')

        return self.db[pid]['qntd']

    # Operação de débito em determinado PID
    # Retorna false se não há quantidade disponível suficiente
    # Levanta exception quanto o pid é desconhecido ou quando a quantidade é negativa
    def debito(self, pid, qntd) -> bool:
        if pid not in self.db:
            raise Exception(f'Debito em PID ({pid}: {type(pid)}) desconhecido\n{list(self.db.keys())}')

        if qntd <= 0:
            raise Exception(f'Debito negativo em {pid}: {qntd}')

        if self.db[pid]['qntd'] < qntd:
            print(f'Falha em debito em {pid}: disponivel {self.db[pid]["qntd"]} debito {qntd}')
            return False

        self.db[pid]['qntd'] -= qntd

        if self.log:
            wrapped_text = util.wrap_color('#ff6347', f'{pid}: -{qntd}')
            self.log_data.insert(0, f'<div>[{datetime.now()}] </div>{wrapped_text}')

        return self.db[pid]['qntd']
