from . import model

def dump_result(result: model.Result):
    print(f"Result({result.test_name}:{result.result_id})")
    print("\tfacts:")
    for fact in result.facts.values():
        print(f"\t\t{fact.name:<30}: {fact.value}")
    print("\tmetrics:")
    for metric in result.metrics:
        print(f"\t\t{metric.name:<30}: {metric.value}")
