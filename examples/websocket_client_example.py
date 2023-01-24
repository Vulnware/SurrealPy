from pprint import pprint
from surrealpy.ws import SurrealClient
from surrealpy.ws.models import LoginParams

# create a new client to connect to SurrealDB
client = SurrealClient("http://localhost:9000/rpc") # You can use SurrealClientThread for thread-safe client


client.connect()

client.login(LoginParams(username="test",password="test"))
client.use(namespace="test",database="test")

def create():
    """An example for creating a new record in table."""
    table = "user"
    data = {"username":"bilinenkisi","email":"bilinenkisi@example.com"}
    response = client.create(table, data)
    pprint(response.result)


def create_with_id():
    """
    Create a record with a specified tid.
    This will raise an exception if the record already exists.
    """
    tid = "user:customidhere" # To specify id you should add ":" after table and then write custom id
    data = {"username":"test","email":"test@example.com"}
    response = client.create(tid, data)
    pprint(response.result)


def find_all():
    """Fetch all records from a table."""
    table = "user"
    response = client.find(table)
    pprint(response.result)


def find_one():
    """Query a table for a specific record by the record's tid."""
    tid = "user:customidhere"
    response = client.find_one(tid)
    pprint(response.result)


def update_one():
    """update a record with a specified tid."""
    tid = "user:customidhere"
    new_data = {"username":"test_updated","email":"test_updated@example.com"}
    response = client.update(tid, new_data)
    pprint(response.result)


def upsert_one():
    """Patch a record with a specified tid."""
    tid = "user:customidhere"
    partial_new_data = {"email":"test_updated@example.com", "notexistfield": "nowitisexist"}
    response = client.update(tid, partial_new_data)
    pprint(response.result)


def delete_all():
    """Delete all records in a table."""
    table = "user"
    client.delete(table)


def delete_one():
    """Delete a record with a specified tid."""
    tid = "user:customidhere"
    client.delete(tid)


def execute_custom_query():
    """Execute a custom query."""
    query = "SELECT * FROM user"
    response = client.query(query)
    pprint(response.result)


def run_all():
    """Run all of the examples."""
    create()
    print()
    create_with_id()
    print()
    find_all()
    print()
    find_one()
    print()
    update_one()
    print()
    upsert_one()
    print()
    delete_one()
    print()
    delete_all()
    print()
    execute_custom_query()


if __name__ == "__main__":
    run_all()