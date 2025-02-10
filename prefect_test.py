from prefect import flow

@flow
def test_flow():
    print("Hello from Prefect!")

if __name__ == "__main__":
    test_flow()