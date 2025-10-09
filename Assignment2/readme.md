# Assignment 2 Information

- Create Virtual Environment:

    ```bash
    # from directory Assignment2/
    py -m venv venv
    pip install -r requirements.txt
    venv\scripts\activate
    ```

- Create .env from .env.example file:

    ```bash
    copy .env.example .env
    ```

- Ensure MongoDb server is installed and for both PostgreSQL and MongoDB connection properties match the .env file

## To Run

```bash
py etl.py
```
