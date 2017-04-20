// Copyright (c) Jeremy Cook. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrcaSql
{
    public class OrcaSqlClient
    {
        private static readonly SortedSet<string> CreatedTables = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

        public string ConnectionString { get; }
        public string Schema { get; }

        protected List<Index> Indices { get; } = new List<Index>();

        public OrcaSqlClient(string connectionString)
        {
            ConnectionString = connectionString;
            Schema = "dbo";
        }

        public async Task<JObject> GetAsync(string collection,
            string where = null,
            string etc = null,
            IDictionary<string, object> parameters = null)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();

                await EnsureTableCreatedAsync(connection, collection);

                var query = new StringBuilder($@"SELECT TOP 1 _document FROM [{Schema}].[{collection}]");
                if (where != null)
                {
                    query.AppendLine($"WHERE ISJSON(_document) > 0 AND ({where})");
                }
                if (etc != null)
                {
                    query.AppendLine(etc);
                }

                var command = new SqlCommand(query.ToString(), connection);
                foreach (var item in parameters)
                {
                    command.Parameters.AddWithValue(item.Key, item.Value);
                }
                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (reader.Read())
                    {
                        string json = reader.GetString(0);
                        return JObject.Parse(json);
                    }
                }

                return null;
            }
        }

        public async Task<IEnumerable<JObject>> GetManyAsync(string collection,
            string where = null,
            string etc = null,
            IDictionary<string, object> parameters = null)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();

                await EnsureTableCreatedAsync(connection, collection);

                var query = new StringBuilder($@"SELECT _document FROM [{Schema}].[{collection}]");
                if (where != null)
                {
                    query.AppendLine($"WHERE ISJSON(_document) > 0 AND ({where})");
                }
                if (etc != null)
                {
                    query.AppendLine(etc);
                }

                var command = new SqlCommand(query.ToString(), connection);
                foreach (var item in parameters)
                {
                    command.Parameters.AddWithValue(item.Key, item.Value);
                }

                var list = new List<JObject>();
                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (reader.Read())
                    {
                        string json = reader.GetString(0);
                        var document = JObject.Parse(json);
                        list.Add(document);
                    }
                }
                return list;
            }
        }

        public async Task<Guid> PostAsync(string collection, object document)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();

                await EnsureTableCreatedAsync(connection, collection);

                Guid _id = Guid.NewGuid();
                var _document = JObject.FromObject(document);

                var command = new SqlCommand($@"INSERT INTO [{Schema}].[{collection}] (_id, _document) VALUES (@_id, @_document)", connection);
                command.Parameters.AddWithValue("_id", _id);
                command.Parameters.AddWithValue("_document", _document.ToString());
                await command.ExecuteNonQueryAsync();

                return _id;
            }
        }

        protected async Task EnsureTableCreatedAsync(SqlConnection connection, string collection)
        {
            string key = $"{ConnectionString}::{Schema}::{collection}";
            if (!CreatedTables.Contains(key))
            {
                var command = new SqlCommand(string.Format(CreateScript, Schema, collection), connection);
                await command.ExecuteNonQueryAsync();

                await EnsureIndexAsync(connection, collection);

                CreatedTables.Add(key);
            }
        }

        protected static string CreateScript { get; } = @"
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{0}].[{1}]') AND type in (N'U'))
BEGIN
    CREATE TABLE [{0}].[{1}](
        [_id] [uniqueidentifier] NOT NULL,
        [_document] [nvarchar](max) NOT NULL,
        CONSTRAINT [PK_{1}] PRIMARY KEY CLUSTERED 
        (
            [_id] ASC
        ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

    IF EXISTS (SELECT * FROM sys.fulltext_catalogs WHERE [name] = 'FT_{0}_{1}')
        DROP FULLTEXT CATALOG [FT_Account]
    CREATE FULLTEXT CATALOG [FT_{1}] WITH ACCENT_SENSITIVITY = ON
    CREATE FULLTEXT INDEX ON [{0}].[{1}] KEY INDEX [PK_{1}] ON ([FT_{0}_{1}]) WITH (CHANGE_TRACKING AUTO)
    ALTER FULLTEXT INDEX ON [{0}].[{1}] ADD ([_document])
    ALTER FULLTEXT INDEX ON [{0}].[{1}] ENABLE
END
";

        private async Task EnsureIndexAsync(SqlConnection connection, string collection)
        {
            var sql = new StringBuilder();
            foreach (var index in Indices.Where(o => o.Collection.Equals(collection, StringComparison.OrdinalIgnoreCase)))
            {
                sql.AppendLine(string.Format(CreateIndexScript,
                    Schema, // 0
                    index.Collection, // 1
                    index.ColumnName, // 2
                    index.JsonFunction, // 3
                    index.IsUnique ? "UNIQUE " : "" // 4
                ));
            }

            var command = new SqlCommand(sql.ToString(), connection);
            await command.ExecuteNonQueryAsync();
        }

        protected static string CreateIndexScript { get; } = @"
IF COL_LENGTH('[{0}].[{1}]', '{2}') IS NULL
BEGIN
    ALTER TABLE [{0}].[{1}] ADD [{2}] AS JSON_VALUE([_document],'{3}')
    CREATE {4}INDEX [IDX_{2}] ON [{0}].[{1}]([{2}])  
END
";
    }
}
