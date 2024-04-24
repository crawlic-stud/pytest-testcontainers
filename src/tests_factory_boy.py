import time
import factory.fuzzy
import pytest
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import sessionmaker
from testcontainers.postgres import PostgresContainer
from factory.alchemy import SQLAlchemyModelFactory
from factory.fuzzy import FuzzyText

Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    username = sqlalchemy.Column(sqlalchemy.VARCHAR(100))


pg_container = PostgresContainer("postgres:16")
pg_container.start()

engine = sqlalchemy.create_engine(pg_container.get_connection_url())
Session = sessionmaker(bind=engine)


class UserFactory(SQLAlchemyModelFactory):
    class Meta:
        model = User
        sqlalchemy_session = Session()
        sqlalchemy_session_persistence = "commit"

    username = FuzzyText()


@pytest.fixture(autouse=True, scope="session")
def lifespan():
    Base.metadata.create_all(engine)
    yield
    pg_container.stop()


@pytest.fixture
def user():
    return UserFactory()


def test_factory_boy_sqlalchemy(user: User):
    with engine.begin() as connection:
        res = connection.execute(sqlalchemy.text("select * from users"))
        user_in_db = res.first()
        print(user_in_db)
        assert user_in_db is not None

    assert user.username
    time.sleep(5)
