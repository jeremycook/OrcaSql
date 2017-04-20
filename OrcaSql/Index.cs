// Copyright (c) Jeremy Cook. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace OrcaSql
{
    public class Index
    {
        public Index(string collection, string jsonFunction, string columnName = null, bool isUnique = false)
        {
            Collection = collection;
            JsonFunction = jsonFunction;
            ColumnName = columnName ?? Regex.Replace(jsonFunction, "[^a-z0-9_]", "_", RegexOptions.IgnoreCase);
            IsUnique = isUnique;
        }

        public string Collection { get; private set; }
        public string JsonFunction { get; private set; }
        public string ColumnName { get; private set; }
        public bool IsUnique { get; private set; }
    }
}
