import pytest
from pydantic import BaseSettings
from abq import bq


class Env(BaseSettings):
    project_name: str

    class Config:
        env_prefix = "test_"
        env_file = ".env"


@pytest.fixture
def credential_file_path():
    return bq.Env().google_application_credentials


@pytest.fixture
def project_name():
    return Env().project_name
