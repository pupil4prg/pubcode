# Dataiku × Snowpark × Modin (pandas on Snowflake) チートシート v2

---

## このドキュメントについて

**対象バージョン**
- Dataiku DSS 14.x
- Snowpark Python 1.40.0以降（2025年時点の最新安定版: 1.44.x）
- pandas on Snowflake (Snowpark pandas / modin互換レイヤー) 同梱

**LLMへのコード生成指示を兼ねる場合の注意点**
- このチートシートはそのままLLMへのコンテキストとして渡せる構成にしている
- 各セクションに「⚠️ LLMへの注意」を記載した箇所は、コード生成時に特に誤りやすいパターン
- バージョン依存の変更点は `[v1.40+]` のように明示している
- コード例は動作確認済みパターンを優先し、非推奨パターンには `# ❌ 非推奨` を付記

---

## 目次

1. [環境セットアップ](#1-環境セットアップ)
2. [セッション管理（Dataiku）](#2-セッション管理dataiku)
3. [pandas on Snowflake の初期化](#3-pandas-on-snowflake-の初期化)
4. [Dataset ↔ Snowpark DataFrame 変換](#4-dataset--snowpark-dataframe-変換)
5. [Snowpark pandas の読み書き](#5-snowpark-pandas-の読み書き)
6. [DataFrame基本操作](#6-dataframe基本操作)
7. [Join / Merge / Concat](#7-join--merge--concat)
8. [フィルタ・条件式](#8-フィルタ条件式)
9. [集計・グループ操作](#9-集計グループ操作)
10. [カラム操作・型変換](#10-カラム操作型変換)
11. [UDF / UDTF / UDAF](#11-udf--udtf--udaf)
12. [Window関数](#12-window関数)
13. [SQL直接発行・DDL](#13-sql直接発行ddl)
14. [MLパイプライン連携](#14-mlパイプライン連携)
15. [パフォーマンス・チューニング](#15-パフォーマンスチューニング)
16. [Dataiku固有パターン](#16-dataiku固有パターン)
17. [エラー対処パターン](#17-エラー対処パターン)
18. [Snowpark pandas 制約・対応状況一覧](#18-snowpark-pandas-制約対応状況一覧)
19. [Hybrid Execution の挙動](#19-hybrid-execution-の挙動)

---

## 1. 環境セットアップ

### Dataikuコードenv（推奨パッケージ）

```
# Snowpark core（必須）
snowflake-snowpark-python>=1.40.0

# pandas on Snowflake（modinバックエンド）を使う場合
snowflake-snowpark-python[modin]
```

対応Pythonバージョン: **3.9 / 3.10 / 3.11**（3.8はSnowpark 1.24.0で廃止、3.12は未対応）

### インポートテンプレート

```python
# recipe_template.py

# === Dataiku ===
import dataiku
from dataiku.snowpark import DkuSnowpark

# === Snowpark core ===
from snowflake.snowpark import functions as F
from snowflake.snowpark.window import Window
from snowflake.snowpark.types import (
    StringType, IntegerType, LongType, FloatType, DoubleType, DecimalType,
    BooleanType, DateType, TimestampType, ArrayType, MapType,
    StructType, StructField
)

# === pandas on Snowflake（modin）を使う場合のみ追加 ===
import modin.pandas as mpd
import snowflake.snowpark.modin.plugin  # セッション自動検出のためインポートするだけでよい [v1.40+]
```

> ⚠️ **LLMへの注意**
> `SnowparkPlugin.init(session)` は旧APIであり、現在は不要。
> `import snowflake.snowpark.modin.plugin` をインポートするだけで、
> アクティブなSnowparkセッションが自動検出される（v1.40+で動作）。

---

## 2. セッション管理（Dataiku）

### セッション取得（Dataiku 14.x / 推奨パターン）

```python
# session_setup.py

import dataiku
from dataiku.snowpark import DkuSnowpark

sp = DkuSnowpark()

# 接続名を明示的に指定（推奨）
session = sp.get_session(connection_name="YOUR_SNOWFLAKE_CONNECTION")

# プロジェクトキーも指定する場合
session = sp.get_session(
    connection_name="YOUR_SNOWFLAKE_CONNECTION",
    project_key=dataiku.default_project_key()
)
```

> ⚠️ **LLMへの注意**
> - `DkuSnowpark()` はインスタンス化が必要（クラスメソッド直接呼び出し不可）
> - `get_session()` には `connection_name` 引数が必須
> - `create_session()` も同じシグネチャで使用可能（同義）

### DkuSnowpark のメソッド一覧（Dataiku 14.x）

| メソッド | 説明 |
|----------|------|
| `get_session(connection_name, project_key=None)` | 既存セッションを返す（または新規作成） |
| `create_session(connection_name, project_key=None)` | 新規セッションを作成 |
| `get_dataframe(dataset, session=None)` | DatasetをSnowpark DataFrameとして返す |
| `write_dataframe(dataset, df, infer_schema=False, force_direct_write=False, dropAndCreate=False)` | DataFrameをDatasetに書き込む |
| `write_with_schema(dataset, df)` | DataFrameをスキーマ推論付きで書き込む（推奨） |

### セッション情報確認

```python
# session_info.py

print(session.get_current_database())
print(session.get_current_schema())
print(session.get_current_warehouse())
print(session.get_current_role())
```

### ウェアハウス・ロール・スキーマの切替

```python
# session_switch.py

session.use_warehouse("LARGE_WH")
session.use_role("ANALYST_ROLE")
session.use_schema("MY_SCHEMA")
session.use_database("MY_DB")
```

### スタンドアロン（Dataikuなし）セッション

```python
# standalone_session.py

from snowflake.snowpark import Session

CONNECTION_PARAMETERS = {
    "account":   "YOUR_ACCOUNT",
    "user":      "YOUR_USER",
    "password":  "YOUR_PASSWORD",
    "role":      "YOUR_ROLE",
    "warehouse": "YOUR_WAREHOUSE",
    "database":  "YOUR_DATABASE",
    "schema":    "YOUR_SCHEMA",
}
session = Session.builder.configs(CONNECTION_PARAMETERS).create()
```

---

## 3. pandas on Snowflake の初期化

### 初期化（v1.40+の正しいパターン）

```python
# modin_init.py

import modin.pandas as mpd
import snowflake.snowpark.modin.plugin  # これだけでセッション自動検出

# セッションが複数ある場合や明示的に使いたい場合
import snowflake.snowpark.modin.plugin as plugin
plugin.init(session)  # 明示指定
```

### Hybrid Executionの制御（v1.40+でデフォルト有効）

```python
# hybrid_execution_control.py

import modin.config as modin_cfg

# Hybrid Executionの確認
print(modin_cfg.IsExperimental.get())  # True = hybrid有効

# Snowflakeへのデータ転送閾値の変更（デフォルト: 100k行）
# SnowflakePandasTransferThreshold 環境変数で制御（コードでの変更は非推奨）
import os
os.environ["SnowflakePandasTransferThreshold"] = "50000"

# Parquet経由の書き込み閾値変更（大規模Series書き込み高速化）
import modin.config
modin.config.PandasToSnowflakeParquetThresholdBytes.put(50 * 1024 * 1024)  # 50MB
```

---

## 4. Dataset ↔ Snowpark DataFrame 変換

### テーブル名の取得

```python
# get_table_name.py

import dataiku

ds = dataiku.Dataset("my_dataset")
info = ds.get_location_info()["info"]

# 部分的なテーブル名（スキーマなしの場合あり）
table_name = info["tableName"]

# 完全修飾名（推奨）
full_table = f'{info["database"]}.{info["schema"]}.{info["tableName"]}'
```

### Snowpark DataFrame として読み込む

```python
# read_as_snowpark_df.py

# 方法1: DkuSnowpark.get_dataframe()（最もシンプル）
df = sp.get_dataframe(dataset=dataiku.Dataset("my_dataset"), session=session)

# 方法2: session.table()（完全修飾名を使用）
df = session.table(full_table)

# 方法3: session.sql()
df = session.sql(f"SELECT * FROM {full_table}")
```

### Snowpark DataFrame → Dataiku Dataset に書き込む（推奨）

```python
# write_snowpark_df.py

output_ds = dataiku.Dataset("output_dataset")

# 推奨: write_with_schema()（スキーマ自動更新）
sp.write_with_schema(output_ds, snowpark_df)

# 代替: write_dataframe()（細かい制御が必要な場合）
sp.write_dataframe(
    output_ds,
    snowpark_df,
    infer_schema=True,     # スキーマ推論
    dropAndCreate=True     # テーブル再作成
)
```

> ⚠️ **LLMへの注意**
> `df.write.mode("overwrite").save_as_table(table_name)` はSnowflakeへの
> 直接書き込みであり、Dataikuのメタデータが更新されない。
> Dataikuのflowでの利用には `sp.write_with_schema()` を使うこと。

### pandas DataFrame ↔ Snowpark DataFrame

```python
# pandas_snowpark_conversion.py

import pandas as pd

# pandas → Snowpark
pdf = pd.DataFrame({"A": [1, 2, 3], "B": ["x", "y", "z"]})
sdf = session.create_dataframe(pdf)

# Snowpark → pandas（全件collectが走る。大テーブルに注意）
pdf = sdf.to_pandas()
pdf = sdf.limit(1000).to_pandas()  # 件数制限推奨

# 型推論エラー回避（TIMESTAMP_NTZ等）
pdf = sdf.to_pandas(infer_with_pandas=False)
```

---

## 5. Snowpark pandas の読み書き

### テーブル読み込み

```python
# modin_read.py

# テーブル名で読み込み
df = mpd.read_snowflake("MY_DB.MY_SCHEMA.MY_TABLE")

# SELECTクエリで読み込み（CTE・ストアドプロシージャ呼び出しも対応）
df = mpd.read_snowflake("SELECT * FROM MY_TABLE WHERE status = 'active'")
df = mpd.read_snowflake(f"SELECT * FROM {full_table} WHERE year >= 2024")

# インデックスカラムを指定
df = mpd.read_snowflake("MY_TABLE", index_col="ID")
df = mpd.read_snowflake("MY_TABLE", index_col=["DATE", "REGION"])

# 取得カラムを絞る
df = mpd.read_snowflake("MY_TABLE", columns=["col1", "col2", "col3"])

# 順序保証を緩和してパフォーマンス向上（結果の行順が不定になる）
df = mpd.read_snowflake("MY_TABLE", enforce_ordering=False)
```

> ⚠️ **LLMへの注意**
> Snowflakeの識別子は大文字小文字が区別されることがある。
> カラム名が小文字のSnowflakeテーブルは `"a"` ではなく `a` で参照する。
> `df.show()` で正規化後のカラム名を確認すること。

### テーブル書き込み

```python
# modin_write.py

# 上書き
df.to_snowflake("MY_DB.MY_SCHEMA.MY_TABLE", if_exists="replace")

# 追記
df.to_snowflake("MY_DB.MY_SCHEMA.MY_TABLE", if_exists="append")

# エラー（既存テーブルがある場合）
df.to_snowflake("MY_DB.MY_SCHEMA.MY_TABLE", if_exists="fail")
```

### Snowpark pandas ↔ Snowpark DataFrame 相互変換

```python
# cross_conversion.py

# Snowpark pandas → Snowpark DataFrame
sdf = mpd_df.to_snowpark()

# Snowpark DataFrame → Snowpark pandas
mpd_df = mpd.from_snowpark(sdf)
```

---

## 6. DataFrame基本操作

### 基本確認

```python
# df_basic_ops.py

df.columns        # カラム名一覧
df.dtypes         # 型一覧
df.head(10)       # 先頭N行（collect発生）
df.describe()     # 統計サマリ（collect発生）

# shape はCOUNTクエリを発行するため大テーブルでは代替手段を検討
df.shape          # (行数, 列数)
session.sql(f"SELECT COUNT(*) FROM {full_table}").collect()  # 代替
```

### カラム選択

```python
# column_select.py

# Snowpark DataFrame
df.select("col1", "col2")
df.select(F.col("col1"), F.col("col2"))

# Snowpark pandas
df[["col1", "col2"]]
```

### カラム追加・更新

```python
# column_add.py

# Snowpark DataFrame
df = df.with_column("new_col", F.col("a") + F.col("b"))
df = df.with_column("new_col", F.lit(0))
df = df.with_column("flag", F.when(F.col("val") > 0, F.lit(True)).otherwise(F.lit(False)))

# 複数カラムを一度に追加（v1.11+）
df = df.with_columns(
    ["col_a", "col_b"],
    [F.col("x") + 1, F.col("y") * 2]
)

# Snowpark pandas
df["new_col"] = df["a"] + df["b"]
df = df.assign(new_col=df["a"] * 2)
```

### カラム削除・リネーム

```python
# column_rename_drop.py

# Snowpark DataFrame
df = df.drop("col1", "col2")
df = df.rename(F.col("old_name"), "new_name")

# Snowpark pandas
df = df.drop(columns=["col1", "col2"])
df = df.rename(columns={"old_name": "new_name"})
```

### 重複削除・ソート・サンプリング

```python
# dedup_sort_sample.py

# Snowpark DataFrame
df = df.distinct()
df = df.sort(F.col("col1").asc(), F.col("col2").desc())
df_sample = df.sample(frac=0.1)
df_sample = df.sample(n=1000)

# Snowpark pandas
df = df.drop_duplicates()
df = df.drop_duplicates(subset=["col1", "col2"])
df = df.sort_values(by=["col1", "col2"], ascending=[True, False])
df_sample = df.sample(frac=0.1)
# ⚠️ sample() は weights / random_state（axis=0時）非対応
# ⚠️ replace=False かつ n > len(df) のとき、pandasと挙動が異なる（エラーにならず全行返す）
```

---

## 7. Join / Merge / Concat

### Snowpark DataFrame の join

```python
# snowpark_join.py

from snowflake.snowpark import functions as F

# inner join（デフォルト）
df_joined = df1.join(df2, df1["id"] == df2["id"])

# left join
df_joined = df1.join(df2, df1["id"] == df2["id"], join_type="left")

# 同名カラムの衝突をsuffixで解決
df_joined = df1.join(
    df2,
    df1["customer_id"] == df2["id"],
    join_type="left",
    lsuffix="_order",
    rsuffix="_customer"
)

# USING句（同名カラムで結合・重複カラムを1つにまとめる）
df_joined = df1.join(df2, ["id"])
df_joined = df1.join(df2, ["date", "region"])

# lateral join（v1.43+）
df_joined = df1.lateral_join(table_function_result)
```

### join_type 一覧

| 値 | SQL相当 |
|----|---------|
| `"inner"`（デフォルト） | INNER JOIN |
| `"left"` / `"leftouter"` | LEFT OUTER JOIN |
| `"right"` / `"rightouter"` | RIGHT OUTER JOIN |
| `"full"` / `"fullouter"` | FULL OUTER JOIN |
| `"cross"` | CROSS JOIN |
| `"semi"` | LEFT SEMI JOIN |
| `"anti"` | LEFT ANTI JOIN |

### Snowpark pandas の merge

```python
# pandas_merge.py

# inner merge
result = df1.merge(df2, on="id", how="inner")

# 異なるカラム名でjoin
result = df1.merge(df2, left_on="order_id", right_on="id", how="left")

# 複数キー
result = df1.merge(df2, on=["date", "product_id"], how="inner")

# 同名カラムの衝突回避
result = df1.merge(df2, on="id", how="left", suffixes=("_a", "_b"))

# ⚠️ how="cross" は Snowpark pandas 非対応 → df1.join(df2, join_type="cross") を使う
```

### 複数テーブルの連続join

```python
# multi_join.py

import functools

# Snowpark DataFrame（チェーン）
result = (
    orders
    .join(customers, orders["customer_id"] == customers["id"], "left")
    .join(products,  orders["product_id"]  == products["id"],  "left")
)

# Snowpark pandas（functools.reduce）
dfs  = [df_orders, df_customers, df_products]
keys = ["customer_id", "product_id"]

result = functools.reduce(
    lambda left, item: left.merge(item[0], left_on=item[1], right_on="id", how="left"),
    zip(dfs[1:], keys),
    dfs[0]
)
```

### Union / Concat

```python
# union_concat.py

# Snowpark DataFrame
df_union = df1.union(df2)           # UNION（重複除去）
df_union = df1.union_all(df2)       # UNION ALL（重複保持）
df_union = df1.union_by_name(df2)   # カラム名でマッチング（列順不一致に対応）

# Snowpark pandas
df_concat = mpd.concat([df1, df2], ignore_index=True)
df_concat = mpd.concat([df1, df2], axis=1)  # 横方向
```

---

## 8. フィルタ・条件式

### 比較・論理演算

```python
# filter_ops.py

# Snowpark DataFrame
df.filter(F.col("age") >= 20)
df.filter((F.col("age") >= 20) & (F.col("status") == "active"))
df.filter((F.col("age") < 20) | (F.col("status") == "inactive"))
df.filter(~(F.col("flag") == True))

# Snowpark pandas
df[df["age"] >= 20]
df[(df["age"] >= 20) & (df["status"] == "active")]
```

### IS NULL / IN / LIKE / BETWEEN

```python
# filter_special.py

# Snowpark DataFrame
df.filter(F.col("col").is_null())
df.filter(F.col("col").is_not_null())
df.filter(F.col("status").isin(["active", "pending"]))
df.filter(~F.col("status").isin(["deleted"]))
df.filter(F.col("name").like("%Smith%"))
df.filter(F.col("email").regexp(r".*@example\.com"))
df.filter(F.col("amount").between(100, 500))

# Snowpark pandas
df[df["col"].isna()]
df[df["col"].notna()]
df[df["status"].isin(["active", "pending"])]
df[df["name"].str.contains("Smith")]
df[df["amount"].between(100, 500)]
```

### CASE WHEN

```python
# case_when.py

# Snowpark DataFrame
df = df.with_column(
    "category",
    F.when(F.col("score") >= 90, F.lit("A"))
     .when(F.col("score") >= 70, F.lit("B"))
     .otherwise(F.lit("C"))
)

# Snowpark pandas（numpy経由）
import numpy as np
conditions = [df["score"] >= 90, df["score"] >= 70]
choices    = ["A", "B"]
df["category"] = np.select(conditions, choices, default="C")
```

### NULL置換

```python
# fillna.py

# Snowpark DataFrame
df = df.fillna({"col1": 0, "col2": "unknown"})
df = df.with_column("col1", F.coalesce(F.col("col1"), F.lit(0)))

# Snowpark pandas
df = df.fillna({"col1": 0, "col2": "unknown"})
```

---

## 9. 集計・グループ操作

### groupby / agg

```python
# groupby_agg.py

# Snowpark DataFrame
result = df.group_by("department").agg(
    F.count("*").alias("cnt"),
    F.sum("salary").alias("total_salary"),
    F.avg("salary").alias("avg_salary"),
    F.max("salary").alias("max_salary"),
    F.min("salary").alias("min_salary"),
    F.stddev("salary").alias("stddev_salary"),
    F.count_distinct("user_id").alias("unique_users"),
)

# group_by_all()（全非集計カラムでグループ化、v1.43+）
result = df.group_by_all().agg(F.sum("sales").alias("total"))

# Snowpark pandas
result = df.groupby("department").agg(
    cnt=("id", "count"),
    total_salary=("salary", "sum"),
    avg_salary=("salary", "mean"),
    unique_users=("user_id", "nunique"),  # nunique対応 [v1.40+]
)
result = df.groupby("department").agg({"salary": ["sum", "mean", "max"]})
```

### pivot_table

```python
# pivot.py

# Snowpark pandas
pivot = df.pivot_table(
    index="department",
    columns="year",
    values="salary",
    aggfunc="sum",
    fill_value=0
)
# aggfunc に "nunique" も指定可能 [v1.40+]
```

### rollup / cube（Snowpark DataFrameのみ）

```python
# rollup_cube.py

result = df.rollup("dept", "year").agg(F.sum("sales").alias("total"))
result = df.cube("dept", "year").agg(F.sum("sales").alias("total"))
```

---

## 10. カラム操作・型変換

### キャスト

```python
# cast.py

# Snowpark DataFrame
df = df.with_column("amount", F.col("amount_str").cast(FloatType()))
df = df.with_column("dt",     F.col("dt_str").cast(TimestampType()))

# Snowpark pandas
df["amount"] = df["amount_str"].astype(float)
df["dt"]     = mpd.to_datetime(df["dt_str"])
```

### 文字列操作

```python
# string_ops.py

# Snowpark DataFrame
df.with_column("col", F.upper(F.col("col")))
df.with_column("col", F.lower(F.col("col")))
df.with_column("col", F.trim(F.col("col")))
df.with_column("col", F.concat(F.col("a"), F.lit("-"), F.col("b")))
df.with_column("len", F.length(F.col("col")))
df.with_column("sub", F.substring(F.col("col"), F.lit(1), F.lit(3)))
df.with_column("rep", F.regexp_replace(F.col("col"), F.lit("[0-9]"), F.lit("")))

# Snowpark pandas
df["col"] = df["col"].str.upper()
df["col"] = df["col"].str.strip()
df["col"] = df["a"] + "-" + df["b"]
df["sub"] = df["col"].str[:3]
df["rep"] = df["col"].str.replace(r"[0-9]", "", regex=True)
```

### 日付操作

```python
# date_ops.py

# Snowpark DataFrame
df.with_column("yr",   F.year(F.col("date")))
df.with_column("mo",   F.month(F.col("date")))
df.with_column("dy",   F.dayofmonth(F.col("date")))
df.with_column("dt2",  F.dateadd("day", F.lit(7), F.col("date")))
df.with_column("diff", F.datediff("day", F.col("start"), F.col("end")))

# Snowpark pandas
df["yr"]   = df["date"].dt.year
df["mo"]   = df["date"].dt.month
df["diff"] = (df["end"] - df["start"]).dt.days
```

### 数値操作

```python
# numeric_ops.py

# Snowpark DataFrame
df.with_column("r",   F.round(F.col("val"), F.lit(2)))
df.with_column("abs", F.abs(F.col("val")))
df.with_column("sq",  F.sqrt(F.col("val")))
df.with_column("pw",  F.pow(F.col("val"), F.lit(2)))
df.with_column("md",  F.mod(F.col("val"), F.lit(3)))
df.with_column("lg",  F.log(F.lit(10), F.col("val")))

# Snowpark pandas
df["r"]  = df["val"].round(2)
df["sq"] = df["val"] ** 0.5
```

### JSON / VARIANT操作

```python
# json_ops.py

# Snowpark DataFrame（VARIANTカラム）
df.with_column("city",     F.get_path(F.col("json_col"), F.lit("address.city")))
df.with_column("city",     F.col("json_col")["address"]["city"])
df.with_column("arr_size", F.array_size(F.col("arr_col")))
df.with_column("elem",     F.get(F.col("arr_col"), F.lit(0)))
```

### 2025年追加の関数

```python
# new_functions_2025.py

# ai_translate（v1.43+）
df.with_column("translated", F.ai_translate(F.col("text"), F.lit("ja")))

# base64エンコード/デコード（v1.43+）
# F.base64_encode(), F.base64_decode_string() 等
```

---

## 11. UDF / UDTF / UDAF

### スカラーUDF（基本）

```python
# udf_basic.py

from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import StringType

@udf(return_type=StringType(), input_types=[StringType()])
def normalize_name(name: str) -> str:
    return name.strip().lower() if name else ""

df = df.with_column("name_norm", normalize_name(F.col("name")))
```

### UDF（サードパーティパッケージ使用）

```python
# udf_packages.py

from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import FloatType

@udf(
    return_type=FloatType(),
    input_types=[FloatType()],
    packages=["numpy", "scikit-learn"]  # Snowflake Anacondaチャンネルから解決
)
def predict_score(val: float) -> float:
    import numpy as np
    return float(np.log1p(val))
```

### UDF（ステージからモデルを参照）

```python
# udf_model_stage.py

from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import FloatType

session.add_import("@my_stage/my_model.pkl")

@udf(return_type=FloatType(), input_types=[FloatType()])
def predict(val: float) -> float:
    import pickle, sys
    import_dir = sys._xoptions.get("snowflake_import_directory")
    with open(f"{import_dir}/my_model.pkl", "rb") as f:
        model = pickle.load(f)
    return float(model.predict([[val]])[0])
```

### Vectorized UDF（pandas UDF相当・高速）

```python
# udf_vectorized.py

from snowflake.snowpark.functions import pandas_udf
from snowflake.snowpark.types import FloatType
import pandas as pd

@pandas_udf(return_type=FloatType(), input_types=[FloatType()])
def fast_normalize(series: pd.Series) -> pd.Series:
    return (series - series.mean()) / series.std()

df = df.with_column("val_norm", fast_normalize(F.col("val")))
```

### UDF永続化

```python
# udf_permanent.py

@udf(
    name="MY_DB.MY_SCHEMA.normalize_name",
    return_type=StringType(),
    input_types=[StringType()],
    replace=True,
    is_permanent=True,
    stage_location="@my_stage"
)
def normalize_name(name: str) -> str:
    return name.strip().lower() if name else ""
```

### UDTF（テーブル関数）

```python
# udtf_basic.py

from snowflake.snowpark.functions import udtf
from snowflake.snowpark.types import StructType, StructField, StringType

class FlattenTags:
    def process(self, tags: str):
        for tag in tags.split(","):
            yield (tag.strip(),)

flatten_tags = udtf(
    FlattenTags,
    output_schema=StructType([StructField("tag", StringType())]),
    input_types=[StringType()]
)
result = df.join_table_function(flatten_tags(F.col("tags_col")))
```

### Secrets API（UDF内での秘密情報参照、v1.40+）

```python
# udf_secrets.py

# UDF/ストアドプロシージャ内でSnowflake Secretsを参照
from snowflake.snowpark import secrets

@udf(return_type=StringType(), input_types=[StringType()])
def call_external_api(input_text: str) -> str:
    from snowflake.snowpark import secrets
    api_key = secrets.get_generic_secret("MY_SECRET_NAME")
    # api_keyを使った処理
    return api_key  # 実際にはAPIコールの結果を返す
```

---

## 12. Window関数

### 基本構文

```python
# window_basic.py

from snowflake.snowpark.window import Window
from snowflake.snowpark import functions as F

window = Window.partition_by("dept").order_by(F.col("salary").desc())
```

### ランキング・LAG/LEAD

```python
# window_rank.py

df = df.with_column("row_num",    F.row_number().over(window))
df = df.with_column("rank",       F.rank().over(window))
df = df.with_column("dense_rank", F.dense_rank().over(window))
df = df.with_column("prev_val",   F.lag(F.col("val"), 1).over(window))
df = df.with_column("next_val",   F.lead(F.col("val"), 1).over(window))
```

### 累積集計・移動平均

```python
# window_cumulative.py

# 累積合計（UNBOUNDED PRECEDING → CURRENT ROW）
cum_window = Window.partition_by("dept").order_by("date").rows_between(
    Window.UNBOUNDED_PRECEDING, Window.CURRENT_ROW
)
df = df.with_column("cum_sum", F.sum("sales").over(cum_window))

# 移動平均（前2行 + 現在行 = 3行）
rolling_window = Window.partition_by("dept").order_by("date").rows_between(-2, 0)
df = df.with_column("rolling_avg", F.avg("sales").over(rolling_window))

# FIRST_VALUE / LAST_VALUE
df = df.with_column("first_val", F.first_value(F.col("val")).over(window))
df = df.with_column("last_val",  F.last_value(F.col("val")).over(
    Window.partition_by("dept").order_by("date")
    .rows_between(Window.UNBOUNDED_PRECEDING, Window.UNBOUNDED_FOLLOWING)
))
```

### Snowpark pandasでのWindow相当

```python
# window_pandas.py

df["rank"]       = df.groupby("dept")["salary"].rank(method="first", ascending=False)
df["cum_sum"]    = df.groupby("dept")["sales"].cumsum()
df["rolling_avg"] = df.groupby("dept")["sales"].transform(
    lambda x: x.rolling(3, min_periods=1).mean()
)
# ⚠️ groupby().transform(lambda...) はSnowflakeにpush downされない場合あり
# → Snowpark DataFrameのWindow関数を優先すること
```

---

## 13. SQL直接発行・DDL

### session.sql()

```python
# sql_direct.py

result = session.sql("SELECT * FROM MY_TABLE WHERE status = 'active'")
result.show()
pdf = result.to_pandas()

# 結果を返さないDDLはcollect()が必要
session.sql("CREATE TABLE IF NOT EXISTS my_table (id INT, name STRING)").collect()
session.sql("TRUNCATE TABLE my_table").collect()
session.sql("DROP TABLE IF EXISTS my_table").collect()
```

### COPY INTO（大規模ファイル転送）

```python
# copy_into.py

session.sql("""
    COPY INTO MY_DB.MY_SCHEMA.MY_TABLE
    FROM @my_stage/data/
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
    )
    ON_ERROR = 'CONTINUE'
""").collect()
```

### DataFrameWriter.save_as_table（直接書き込み）

```python
# save_as_table.py

# 上書き
df.write.mode("overwrite").save_as_table("MY_DB.MY_SCHEMA.MY_TABLE")

# 追記
df.write.mode("append").save_as_table("MY_DB.MY_SCHEMA.MY_TABLE")

# 対象削除+挿入（overwrite_condition、v1.44+）
df.write.mode("overwrite").save_as_table(
    "MY_DB.MY_SCHEMA.MY_TABLE",
    overwrite_condition=F.col("year") == 2024  # 2024年分のみ削除して再挿入
)
```

### DB-API（v1.40+ GA）

```python
# dbapi.py

# 外部DBから直接Snowpark DataFrameに読み込む
import psycopg2

df = session.read.dbapi(
    create_connection=psycopg2.connect,
    connection_parameters={
        "host": "localhost", "dbname": "mydb",
        "user": "user", "password": "pass"
    },
    table="source_table"
)
```

### ストアドプロシージャ呼び出し

```python
# stored_proc.py

session.call("MY_STORED_PROC", arg1, arg2)
```

---

## 14. MLパイプライン連携

### Snowpark ML（前処理）

```python
# snowpark_ml_preprocessing.py

from snowflake.ml.modeling.preprocessing import (
    StandardScaler, MinMaxScaler, OrdinalEncoder, OneHotEncoder
)

scaler = StandardScaler(
    input_cols=["feature1", "feature2"],
    output_cols=["feature1_scaled", "feature2_scaled"]
)
df_scaled = scaler.fit(df).transform(df)
```

### Snowpark ML（モデル学習・推論）

```python
# snowpark_ml_training.py

from snowflake.ml.modeling.xgboost import XGBClassifier

model = XGBClassifier(
    input_cols=["feat1", "feat2", "feat3"],
    label_cols=["label"],
    output_cols=["prediction"]
)
model.fit(df_train)
df_pred = model.predict(df_test)
```

### Snowpark ML Model Registry

```python
# model_registry.py

from snowflake.ml.registry import Registry

reg = Registry(session=session, database_name="MY_DB", schema_name="MY_SCHEMA")

# モデル登録
model_version = reg.log_model(
    model=model,
    model_name="my_model",
    version_name="v1",
    sample_input_data=df_train.limit(10)
)

# 取得・推論
mv = reg.get_model("my_model").version("v1")
predictions = mv.run(df_test, function_name="predict")
```

### Dataiku Saved Model + MLflow 連携

```python
# dataiku_mlflow.py

import mlflow

# DataikuのExperiment Trackingで登録されたモデルを参照
model_uri = f"models:/my_model/Production"
loaded_model = mlflow.sklearn.load_model(model_uri)

# sklearn モデルをSnowparkのUDFでラップして推論
import pickle
session.file.put_stream(
    pickle.dumps(loaded_model),
    "@my_stage/model.pkl",
    auto_compress=False
)
```

---

## 15. パフォーマンス・チューニング

### 実行計画確認

```python
# query_plan.py

df.explain()  # SQLクエリプランを出力（Snowflake側での実行計画確認）
```

### lazy evaluation とcollectのタイミング

```python
# lazy_eval.py

# 以下の操作はSnowflakeにクエリを発行する（collectが走る）
df.collect()              # 全件取得
df.show()                 # 先頭N行表示
df.to_pandas()            # pandas変換
df.count()                # 行数カウント（個別クエリ発行）
df.write.save_as_table()  # 書き込み

# 以下はlazy（クエリを発行しない）
df.filter(...)
df.select(...)
df.with_column(...)
df.join(...)
df.group_by(...)
```

### キャッシュ・中間マテリアライズ

```python
# cache.py

# Snowpark DataFrame: 一時テーブルに保存して再利用
df_cached = df.cache_result()

# Snowpark pandas: 中間結果をテーブルに書き出して再読み込み
df.to_snowflake("MY_TEMP_TABLE", if_exists="replace")
df_reload = mpd.read_snowflake("MY_TEMP_TABLE")
```

### CTE最適化（v1.40+でデフォルト有効）

```python
# cte_optimization.py

# cte_optimization_enabled はSnowpark pandasセッションでデフォルトTrue
# 手動で無効化する場合
session.conf.set("cte_optimization_enabled", False)
```

### カラムプルーニング・フィルタ早期適用

```python
# pushdown_tips.py

# 早期にカラムを絞る（push downの効果を最大化）
df = df.select("col1", "col2", "col3")

# Snowflakeのクラスタリングキーに沿ったフィルタを先に適用
df = df.filter(F.col("date") >= "2024-01-01")  # クラスタリングキーがdateの場合
df = df.filter(F.col("region") == "JP")
```

### Snowpark pandasのpush down確認

```python
# pushdown_debug.py

import logging
logging.getLogger("snowflake.snowpark.modin").setLevel(logging.DEBUG)
```

---

## 16. Dataiku固有パターン

### Pythonレシピの基本構造（推奨パターン）

```python
# dataiku_recipe_template.py

import dataiku
from dataiku.snowpark import DkuSnowpark
from snowflake.snowpark import functions as F

# セッション初期化
sp = DkuSnowpark()
session = sp.get_session(connection_name="YOUR_CONNECTION")

# Input読み込み（推奨: get_dataframe）
ds_in = dataiku.Dataset("input_dataset")
df = sp.get_dataframe(dataset=ds_in, session=session)

# 処理
df = df.filter(F.col("active") == True)
df = df.with_column("processed_at", F.current_timestamp())

# Output書き込み（推奨: write_with_schema）
ds_out = dataiku.Dataset("output_dataset")
sp.write_with_schema(ds_out, df)
```

### pandas on Snowflakeを使うレシピ

```python
# dataiku_modin_recipe.py

import dataiku
from dataiku.snowpark import DkuSnowpark
import modin.pandas as mpd
import snowflake.snowpark.modin.plugin

sp = DkuSnowpark()
session = sp.get_session(connection_name="YOUR_CONNECTION")

# Input
ds_in = dataiku.Dataset("input_dataset")
info  = ds_in.get_location_info()["info"]
full_table = f'{info["database"]}.{info["schema"]}.{info["tableName"]}'
df = mpd.read_snowflake(full_table)

# 処理（pandas APIで記述）
df = df[df["amount"] > 0]
df["amount_log"] = df["amount"].apply(lambda x: x ** 0.5)  # 注意: push downされない場合あり

# Output（Snowpark DataFrameに変換してから書き込み）
ds_out = dataiku.Dataset("output_dataset")
sp.write_with_schema(ds_out, df.to_snowpark())
```

### Dataiku変数の参照

```python
# dataiku_variables.py

import dataiku

client  = dataiku.api_client()
project = client.get_project(dataiku.default_project_key())
variables = project.get_variables()

# 標準変数
my_var = variables["standard"]["my_variable"]

# カスタム変数
custom_vars = dataiku.get_custom_variables()
```

### Pluginレシピのパラメータ取得

```python
# plugin_params.py

params = get_recipe_config()
flag     = params.get("my_boolean_param", False)
selected = params.get("my_multiselect_param", [])
```

### 動的フィルター条件構築（functools.reduce）

```python
# dynamic_filter.py

import functools
from snowflake.snowpark import functions as F

conditions = [
    F.col("region").isin(selected_regions) if selected_regions else None,
    F.col("year") >= start_year           if start_year else None,
    F.col("status") == "active",
]
conditions = [c for c in conditions if c is not None]

if conditions:
    combined = functools.reduce(lambda a, b: a & b, conditions)
    df = df.filter(combined)
```

---

## 17. エラー対処パターン

### SnowparkJoinException: ambiguous column

```python
# fix_ambiguous_column.py

# 原因: join時に両テーブルに同名カラムが存在

# 対処1: suffixを指定
df_joined = df1.join(df2, df1["id"] == df2["id"], lsuffix="_l", rsuffix="_r")

# 対処2: joinの前にリネーム
df2 = df2.rename(F.col("id"), "id_right")
df_joined = df1.join(df2, df1["id"] == df2["id_right"])

# 対処3: USING句（同名カラムを1つにまとめる）
df_joined = df1.join(df2, ["id"])
```

### pandas型推論エラー（TIMESTAMP_NTZ等）

```python
# fix_type_inference.py

pdf = sdf.to_pandas(infer_with_pandas=False)  # Snowflake型をそのまま使用
```

### Snowpark pandas のFallback（push down非対応操作）

```python
# fix_fallback.py

# ❌ 非推奨: apply()はfallbackしやすい
df["col"] = df["col"].apply(lambda x: x.strip())

# ✅ 推奨: Snowpark関数に置き換える
df = df.with_column("col", F.trim(F.col("col")))

# 同様にstr.lower()等もSnowpark関数に置き換えを検討
df["col"] = df["col"].str.lower()           # ❌ fallbackの可能性
df = df.with_column("col", F.lower(F.col("col")))  # ✅ push down
```

### SnowparkSQLException: Object not found

```python
# fix_object_not_found.py

# ❌ 非推奨: スキーマなしのテーブル名
df = session.table("MY_TABLE")

# ✅ 推奨: 完全修飾名
df = session.table("MY_DB.MY_SCHEMA.MY_TABLE")
```

### `SnowparkPlugin.init()` の不要化（v1.40+）

```python
# fix_plugin_init.py

# ❌ 旧API（廃止ではないが不要）
from snowflake.snowpark.modin.plugin import SnowparkPlugin
SnowparkPlugin.init(session)

# ✅ 現在の推奨（import するだけでアクティブセッションを自動検出）
import snowflake.snowpark.modin.plugin
```

### Snowpark pandas のメモリ問題（大テーブルの統計確認）

```python
# fix_memory.py

# ❌ 全件collectが走る
df.shape
df.describe()

# ✅ 代替手段
session.sql(f"SELECT COUNT(*) FROM {full_table}").collect()
df.limit(10000).describe()  # サンプルで確認
```

---

## 18. Snowpark pandas 制約・対応状況一覧

| 操作 | 対応状況 | 推奨代替手段 |
|------|----------|-------------|
| `apply()` 行/列方向 | △ push down非保証 | `pandas_udf` または Snowpark DataFrame API |
| `applymap()` / `map()` | △ | `F.xxx()` 関数群 |
| `merge(how="cross")` | ✗ 未対応 | `df1.join(df2, join_type="cross")` |
| `groupby().apply()` | △ push down非保証 | `pandas_udf` に変換 |
| `rolling().apply()` | ✗ | Window関数 + UDF |
| `MultiIndex` | ✗ 未対応 | フラットカラム設計 |
| `pivot()` | △ | `pivot_table()` を使用 |
| `to_csv()` / `to_json()` ローカル保存 | ✗ | `to_pandas().to_csv()` または COPY INTO |
| `read_csv()` ローカルファイル | ✗ | ステージ経由 `@stage/file.csv` |
| `sample(weights=...)` | ✗ axis=0時 | Snowpark DataFrame `.sample()` |
| `cumsum(axis=1)` | ✗ | Snowpark DataFrame のWindow関数 |
| `skew(axis=1)` | ✗ | pandas fallback（小テーブルのみ） |
| `get_dummies(dummy_na=True)` | △ pandas fallback | Snowpark ML の OneHotEncoder |
| `sparse` dtype | ✗ | 非対応 |
| `Categorical` dtype | △ | `StringType` / `OrdinalEncoder` |
| `MultiIndex` columns | ✗ | フラットカラム |
| `DatetimeTZDtype` | △ | `TimestampType(tz)` を明示 |

---

## 19. Hybrid Execution の挙動

v1.40.0以降、**Hybrid Execution がデフォルト有効**。  
Snowflake側とローカルpandas側の両エンジンを自動選択する。

### 切り替えルール（自動）

| 条件 | 使用エンジン |
|------|-------------|
| インメモリPythonデータから作成されたDF | ローカルpandas |
| `mpd.read_snowflake()` で作成されたDF | Snowflakeエンジン |
| 2つのDFをまたぐ操作（異なるバックエンド） | データ転送コストが低い側 |
| 行数がしきい値以下（デフォルト100k行） | ローカルpandasへ自動切替 |

### 重要な特性

- DFの型は常に `modin.pandas.DataFrame` で変わらない（バックエンドが切り替わっても）
- フィルタ後の行数推定は近似値のため、最適バックエンドに即座に切り替わらない場合がある
- 転送しきい値は `SnowflakePandasTransferThreshold` 環境変数で変更可能

### Hybrid Executionを無効化（全処理をSnowflakeで強制実行）

```python
# disable_hybrid.py

import os
# セッション開始前に設定が必要
os.environ["MODIN_IN_MEMORY_PANDAS_THRESHOLD"] = "0"
```

---

*最終更新: 2025年3月 / Snowpark Python 1.44.x・Dataiku DSS 14.x準拠*  
*公式ドキュメント: https://docs.snowflake.com/en/developer-guide/snowpark/python/pandas-on-snowflake*  
*Dataiku Snowpark API: https://developer.dataiku.com/latest/api-reference/python/snowpark.html*
