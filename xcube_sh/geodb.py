# The MIT License (MIT)
# Copyright (c) 2019 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import os
import os.path
from abc import abstractmethod, ABCMeta
from typing import Optional, Sequence, Dict, Any, Union
import fiona as fio
import psycopg2
import geopandas as gpd
import json


Feature = Dict[str, Any]
Schema = Dict[str, Any]
BBox = Sequence


class GeoDBService(metaclass=ABCMeta):


    @abstractmethod
    def find_feature(self, collection_name: str, query: str, bbox: BBox = None, bbox_mode: str = 'contains') -> \
            Optional[Feature]:
        """

        :param bbox_mode: Can be 'contains' or 'within'. Refer to https://postgis.net/docs/ST_Within.html and https://postgis.net/docs/ST_Contains.html
        :param bbox: bbox as array [minx, miny, maxx, maxy]
        :param collection_name:
        :param query: a query to filter features from all collections
        """

    @abstractmethod
    def find_features(self, collection_name: str, query: str, max_records: int, fmt: str, bbox: BBox = None,
                      bbox_mode: str = 'contains') -> \
            Union[Sequence[Feature], gpd.GeoDataFrame]:
        """

        :param bbox_mode: bbox_mode: Can be 'contains' or 'within'. Refer to https://postgis.net/docs/ST_Within.html and https://postgis.net/docs/ST_Contains.html
        :param bbox: bbox as array [minx, miny, maxx, maxy]
        :param collection_name: Name of the collection
        :param fmt: format of return type
        :param query: a query to filter features from all collections
        :param max_records: maximum number of records to be returned
        """

    @abstractmethod
    def new_collection(self, collection_name: str, schema: Schema):
        """

        :param collection_name: a name for teh new collection
        :param schema: a feature schema
        """

    @abstractmethod
    def drop_collection(self, collection_name: str):
        """

        :param collection_name: a name for teh new collection
        """

    @abstractmethod
    def add_feature(self, collection_name: str, feature: Feature) -> str:
        """

        :param collection_name: the name of the collection the feature will be added to
        :param feature: a feature to be added
        """

    @abstractmethod
    def add_features(self, collection_name: str, features: Sequence[Feature]) -> str:
        """

        :param collection_name: the name of the collection the features will be added to
        :param features: a list of features to be added
        """
        pass


class LocalGeoDBService(GeoDBService):

    def __init__(self):
        super().__init__()

    def find_feature(self, collection_name: str, query: str, bbox: BBox = None, bbox_mode: str = 'contains') -> \
            Optional[Feature]:
        features = self.find_features(collection_name, query, max_records=1)
        return features[0] if features else None

    def find_features(self, collection_name: str, query: str = None, max_records: int = -1, fmt: str = 'geojson',
                      bbox: BBox = None, bbox_mode: str = 'contains') -> \
            Union[Sequence[Feature], gpd.GeoDataFrame]:

        if bbox:
            raise NotImplementedError("bbox feature not implemented for driver local")

        compiled_query = compile(query, 'query', 'eval')
        result_set = []

        collection = self._get_collection(collection_name)
        for feature in collection:
            # noinspection PyBroadException
            try:
                _locals = dict(id=feature.get('id')) if feature.get('id') else {}
                _locals.update(feature.get('properties', {}))
                result = eval(compiled_query, None, _locals)
            except Exception:
                result = False
            if result:
                result_set.append(feature)
                if len(result_set) >= max_records:
                    break
        return result_set

    def new_collection(self, collection_name: str, schema: Schema):
        raise NotImplementedError("new_collection not yet implemented")

    def drop_collection(self, collection_name: str):
        raise NotImplementedError("drop_collection not yet implemented")

    def add_feature(self, collection_name: str, feature: Feature) -> str:
        raise NotImplementedError("add_feature not yet implemented")

    def add_features(self, collection_name: str, features: Sequence[Feature]) -> str:
        raise NotImplementedError("new_collection not yet implemented")

    def _get_collection(self, collection_name: str):
        source_path = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'geodb'))
        file_path = os.path.join(source_path, collection_name + '.geojson')
        if os.path.isfile(file_path):
            return fio.open(file_path)
        else:
            raise FileNotFoundError(f"Could not find file {collection_name}.geojson")


# noinspection SqlNoDataSourceInspection
class RemoteGeoPostgreSQLService(GeoDBService):
    _FILTER_SQL = """SELECT  json_build_object(
        'type', 'Feature',
	    'properties', properties::json,
        'geometry', ST_AsGeoJSON(geometry)::json
        )
        FROM "{table_prefix}{collection}" 
        WHERE {query} {max}
        """

    _FILTER_LONG_SQL = """SELECT  *
        FROM "{table_prefix}{collection}" 
        WHERE {query} {max}
    """

    _GET_TABLES_SQL = """SELECT t.table_name
        FROM information_schema.tables t
        INNER JOIN information_schema.columns c on c.table_name = t.table_name 
                                        and c.table_schema = t.table_schema
        WHERE c.column_name = 'properties'
              AND t.table_schema not in ('information_schema', 'pg_catalog')
              AND t.table_type = 'BASE TABLE'
              
        ORDER BY t.table_schema;
    """

    _TABLE_EXISTS_SQL = """SELECT EXISTS (
            SELECT 1
            FROM   information_schema.tables
            WHERE  table_schema = 'public'
            AND    table_name = '{table_prefix}{collection}')
            """

    _DROP_COLLECTION_SQL = "DROP TABLE {table_prefix}{collection}"

    _CREATE_COLLECTION_SQL = """
            -- Table: public.{table_prefix}{collection}

            -- DROP TABLE public.{table_prefix}{collection};
            
            CREATE TABLE public.{table_prefix}{collection}
            (
                -- Inherited from table public.{table_prefix}master: id integer NOT NULL DEFAULT nextval('{table_prefix}id_seq1'::regclass),
                -- Inherited from table public.{table_prefix}master: properties json,
                -- Inherited from table public.{table_prefix}master: name character varying(512) COLLATE pg_catalog."default",
                -- Inherited from table public.{table_prefix}master: geometry geometry,
                -- Inherited from table public.{table_prefix}master: type character varying COLLATE pg_catalog."default" NOT NULL
                {columns}
            )
                INHERITS (public.{table_prefix}master)
            WITH (
                OIDS = FALSE
            )
            TABLESPACE pg_default;
            
            ALTER TABLE public.{table_prefix}{collection}
                OWNER to postgres;
            """

    _TABLE_PREFIX = 'geodb_'

    def __init__(self, host: str, user: Optional[str] = None, password: Optional[str] = None, port: int = 5432,
                 conn: object = None):
        """

        :param host: Host of database
        :param user: user name
        :param password: password
        :param port: port (default: 5432)
        """
        super().__init__()

        if not user:
            user = os.getenv("PSQL_USER")
        if not password:
            user = os.getenv("PSQL_PASSWD")

        if conn:
            self._conn = conn
        else:
            self._conn = psycopg2.connect(f"host={host} port={port} user={user} password={password}")

        self._collections = self._get_collections()
        self._sql = None

    @property
    def collections(self) -> Optional[Sequence[str]]:
        return self._collections

    @property
    def sql(self) -> str:
        return self._sql

    def find_feature(self, collection_name: str, query: str, bbox: BBox = None, bbox_mode: str = 'contains') -> \
            Optional[Feature]:
        features = self.find_features(collection_name, query, max_records=1)
        return features[0] if features else None

    def _alter_query(self, query, bbox, bbox_mode, fmt):
        bbox_query = None
        if bbox:
            minx = bbox[0]
            miny = bbox[1]
            maxx = bbox[2]
            maxy = bbox[3]
            bbox = f"POLYGON(({minx} {miny},{minx} {maxy},{maxx} {maxy},{maxx} {miny},{minx} {miny}))::geometry"
            if bbox_mode == 'contains':
                bbox_query = f" ST_Contains('{bbox}', geometry)"
            elif bbox_mode == 'within':
                bbox_query = f" ST_Within('{bbox}', geometry)"
            else:
                raise ValueError(f"bbox_mode {bbox_mode} unknown")

        if not query and not bbox_query:
            query = 'TRUE'
        elif query and not bbox_query:
            if fmt == 'geojson':
                query = f"properties->>{query}"
            elif fmt == 'gdf':
                query = f"{query}"
            else:
                raise ValueError(f"format {fmt} not known")
        elif not query and bbox_query:
            query = bbox_query
        elif query and bbox_query:
            if fmt == 'geojson':
                query = f"properties->>{query} and {bbox_query}"
            elif fmt == 'gdf':
                query = f"{query} and {bbox_query}"
            else:
                raise ValueError(f"format {fmt} not known")
        return query

    def find_features(self, collection_name: str, query: str = None, max_records: int = -1, fmt: str = 'geojson',
                      bbox: BBox = None, bbox_mode: str = 'contains') -> Union[Sequence[Feature], gpd.GeoDataFrame]:
        if not self._collection_exists(collection_name=collection_name):
            raise ValueError(f"Collection {collection_name} not found")

        limit = ''
        if max_records > -1:
            limit = 'LIMIT ' + str(max_records)

        query = self._alter_query(query=query, bbox=bbox, bbox_mode=bbox_mode, fmt=fmt)

        if fmt == 'geojson':
            self._sql = self._FILTER_SQL.format(collection=collection_name, max=limit, query=query,
                                                table_prefix=self._TABLE_PREFIX)
            cursor = self._conn.cursor()
            cursor.execute(self._sql)

            result_set = []
            for f in cursor.fetchall():
                result_set.append(f[0])
            return result_set
        elif fmt == 'gdf':
            self._sql = self._FILTER_LONG_SQL.format(collection=collection_name, max=limit, query=query,
                                                     table_prefix=self._TABLE_PREFIX)
            return gpd.GeoDataFrame.from_postgis(self._sql, self._conn, geom_col='geometry')
        else:
            raise ValueError(f"format {fmt} unknown")

    def new_collection(self, collection_name: str, schema: Schema) -> str:
        if self._collection_exists(collection_name):
            raise ValueError(f"Collection {collection_name} exists")

        columns = []
        for k, v in schema['properties'].items():
            columns.append(self._make_column(k, v))

        sql = self._CREATE_COLLECTION_SQL.format(collection=collection_name, columns=',\n'.join(columns),
                                                 table_prefix=self._TABLE_PREFIX)
        self.query(sql)

        return "Collection created"

    def drop_collection(self, collection_name: str):
        if not self._collection_exists(collection_name=collection_name):
            raise ValueError(f"Collection {collection_name} does not exist")

        sql = self._DROP_COLLECTION_SQL.format(collection=collection_name, table_prefix=self._TABLE_PREFIX)
        self.query(sql=sql)

    def add_feature(self, collection_name: str, feature: Feature) -> str:
        self.add_features(collection_name, [feature])
        return "Feature Added"

    def add_features(self, collection_name: str, features: Sequence[Feature]) -> str:
        for f in features:

            _local = f['properties'].keys()
            columns = []
            for c in _local:
                columns.append(f'"{c.lower()}"')

            _local = f['properties'].values()
            values = []
            for v in _local:
                if isinstance(v, float) or isinstance(v, int) or isinstance(v, bool):
                    values.append(f"{v}")
                elif v is None:
                    values.append(f"null")
                else:
                    values.append(f"'{str(v)}'")

            columns = ','.join(columns)
            values = ','.join(values)
            geometry = f['geometry']
            properties = f['properties']

            sql = f"INSERT INTO {self._TABLE_PREFIX}{collection_name}(properties, name, {columns}, geometry) " \
                f"VALUES('{json.dumps(properties)}', '{properties['S_NAME']}', {values}, ST_GeomFromGeoJSON('{json.dumps(geometry)}')) "
            self.query(sql=sql)
        return "Features Added"

    def query(self, sql: str) -> Optional[Any]:
        """

        Args:
            sql: The raw SQL statement in PostgreSQL dialect

        Returns:
            A list of tuples if the number of returned rows is larger than one or a single tuple otherwise, or
            nothing if the query is not a SELECT statement


        """
        cur = self._conn.cursor()
        cur.execute(sql)

        if "SELECT" in sql:
            if cur.rowcount == 1:
                result = cur.fetchone()
            else:
                result = cur.fetchall()
        else:
            self._conn.commit()
            result = True

        cur.close()
        return result

    def _get_collections(self):
        result = self.query(self._GET_TABLES_SQL)
        return [r[0] for r in result]

    def _collection_exists(self, collection_name: str):
        return self._TABLE_PREFIX + collection_name in self._collections

    def _make_column(self, name: str, typ: str):
        if typ == 'str':
            col_create_str = f'{name} character varying(256) COLLATE pg_catalog."default"'
        elif 'int' in typ:
            col_create_str = f'{name} integer'
        elif 'float' in typ:
            prec_str = ""
            _local = typ.split(':')
            if len(_local) == 2:
                _local = _local[1].split('.')
            if len(_local) == 2:
                prec_str = f"({_local[0]},{_local[1]})"
            col_create_str = f'{name} numeric{prec_str}'
        else:
            raise NotImplementedError(f"Column type {typ} not implemented")

        return col_create_str

    def _make_insert_column(self, name: str, typ: str):
        if typ == 'str':
            col_create_str = f'{name} character varying(256) COLLATE pg_catalog."default"'
        elif 'int' in typ:
            col_create_str = f'{name} integer'
        elif 'float' in typ:
            prec_str = ""
            _local = typ.split(':')
            if len(_local) == 2:
                _local = _local[1].split('.')
            if len(_local) == 2:
                prec_str = f"({_local[0]},{_local[1]})"
            col_create_str = f'{name} numeric{prec_str}'
        else:
            raise NotImplementedError(f"Column type {typ} not implemented")

        return col_create_str


def get_geo_db_service(driver: str = 'local', **kwargs) -> GeoDBService:
    """

    :param driver: 
    :param kwargs: Parameter for subsequence service
    :return:
    """
    if driver == 'local':
        return LocalGeoDBService()
    else:
        return RemoteGeoPostgreSQLService(**kwargs)
