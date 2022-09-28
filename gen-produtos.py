import random
import yaml


def main():
    qntd_produtos = 210

    classes = [
        {"nome": "A", "peso": 100},
        {"nome": "B", "peso": 60},
        {"nome": "C", "peso": 20}
    ]

    sorteio_classes = random.choices(
        [c['nome'] for c in classes],
        weights=[c['peso'] for c in classes],
        k=qntd_produtos
    )

    produtos = []

    res = {"A": 0, "B": 0, "C": 0}

    for i in range(0, qntd_produtos):
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
