from typing import Any

from geoalchemy2 import Geometry
from geoalchemy2.shape import from_shape
from shapely.geometry import Point
from sqlalchemy import Column, Integer, MetaData, Table, create_engine, text
from sqlalchemy.event import listen

metadata = MetaData()


def get_points_table(schema: str) -> Table:
    return Table(
        "points",
        metadata,
        Column("point_id", Integer, primary_key=True),
        Column(
            "position",
            Geometry(
                "POINTZ", srid=4326, dimension=3, management=True, spatial_index=False
            ),
        ),
        # TODO: Find out why the code below crashes while attempting to write data, while using spatial_index
        # Column("position", Geometry("POINTZ", srid=4326, dimension=3, management=True)),
        schema=schema,
        keep_existing=True,
    )


def main() -> None:
    filename = "test.sqlite"
    engine = create_engine(f"sqlite:///{filename}", echo=False)
    listen(engine, "connect", _load_sqlite_spatialite_extension)
    with engine.connect() as conn:
        # Create the file, and start by initializing Spatialite
        conn.execute(text("SELECT InitSpatialMetaData(1);"))

        # Generate the tables
        get_points_table("")
        metadata.create_all(conn)

        # Add one entry
        conn.execute(
            get_points_table("")
            .insert()
            .values(point_id=1234, position=from_shape(Point(128.0, 62.0, 153.0)))
        )

        # Commit the current transaction to write to the file.
        conn.get_transaction().commit()


def _load_sqlite_spatialite_extension(dbapi_conn: Any, _: Any) -> None:
    """
    This loads the Spatialite Extension for SQLite.
    """
    dbapi_conn.enable_load_extension(True)
    dbapi_conn.load_extension("mod_spatialite")


if __name__ == "__main__":
    main()
