from copy import deepcopy

from suthing import FileHandle

from graph_cast.db.onto import ConnectionKind, DBConnectionConfig, WSGIConfig


class ConfigFactory:
    @classmethod
    def create_config(
        cls, path=None, dict_like=None
    ) -> DBConnectionConfig | WSGIConfig:
        if path is not None:
            config = FileHandle.load(path)
        elif dict_like is not None and isinstance(dict_like, dict):
            config = deepcopy(dict_like)
        else:
            raise ValueError(
                "At least one of args should be non None : secret_path or args"
            )

        db_type = config.pop("db_type", None)
        try:
            dbt = ConnectionKind(db_type)
        except ValueError as e:
            raise ValueError(
                " `db_type` found in config not supported: should be in"
                f" {set(ConnectionKind)} : {e}"
            )

        return dbt.config_class()(**config)
