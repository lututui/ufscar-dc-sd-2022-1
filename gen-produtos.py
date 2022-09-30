import random
import yaml

import util


def main():
    classes = [
        {"nome": "A", "peso": 100},
        {"nome": "B", "peso": 60},
        {"nome": "C", "peso": 20}
    ]

    sorteio_classes = random.choices(
        [c['nome'] for c in classes],
        weights=[c['peso'] for c in classes],
        k=util.qntd_produtos
    )

    produtos = []

    res = {"A": 0, "B": 0, "C": 0}

    for i in range(0, util.qntd_produtos):
        p = {
            "pid": i + 1,
            "classe": sorteio_classes[i]
        }
        produtos.append(p)

        res[sorteio_classes[i]] += 1

    with open('produtos.yaml', 'w') as f:
        yaml.dump(produtos, f)

    print(res)


if __name__ == '__main__':
    main()
