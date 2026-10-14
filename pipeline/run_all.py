import sys

from pipeline import backfill, check, consumer, reconcile

STEPS = [("backfill", backfill, []), ("consumer", consumer, []), ("reconcile", reconcile, ["--repair"]),
         ("consumer (stores any repairs)", consumer, []), ("check", check, [])]


def main():
    for name, module, args in STEPS:
        print(f"\n===== {name} =====")
        sys.argv = [name] + args              
        module.main()


if __name__ == "__main__":
    main()