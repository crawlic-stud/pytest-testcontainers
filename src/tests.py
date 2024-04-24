import pytest
import sqlalchemy
from pymongo import MongoClient
from redis import Redis

import sqlalchemy.orm
from testcontainers.postgres import PostgresContainer
from testcontainers.mongodb import MongoDbContainer
from testcontainers.redis import RedisContainer


@pytest.fixture
def postgres_connection():
    with PostgresContainer("postgres:16") as postgres:
        engine = sqlalchemy.create_engine(postgres.get_connection_url())
        with engine.begin() as connection:
            yield connection


def test_postgres(postgres_connection: sqlalchemy.Connection):
    result = postgres_connection.execute(sqlalchemy.text("select version()"))
    (version,) = result.fetchone()
    assert "PostgreSQL 16" in version


@pytest.fixture
def mongo_client():
    with MongoDbContainer("mongo") as mongo:
        client = mongo.get_connection_client()
        yield client


def test_mongo(mongo_client: MongoClient):
    collection = mongo_client.test.collection
    collection.insert_one({"name": "test"})
    result = collection.find_one({"name": "test"})
    assert result["name"] == "test"


@pytest.fixture
def redis_client():
    with RedisContainer("redis:7.2") as redis_container:
        redis = redis_container.get_client()
        yield redis


def test_redis(redis_client: Redis):
    redis_client.set("name", "test")
    result = redis_client.get("name")
    assert result == b"test"
