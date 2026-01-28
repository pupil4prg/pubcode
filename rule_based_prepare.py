# -*- coding: utf-8 -*-
import dataiku
import pandas as pd
import json

def create_rule_based_prepare_recipe(input_dataset_name, condition_dataset_name, output_dataset_name, recipe_name):
    """
    条件データセットに基づいて、DataikuのPrepareレシピを動的に作成します。
    
    条件データセット(Dataset B)には以下のカラムが含まれていることを想定しています:
    - Group (グループ): 出力されるフラグカラム名
    - Column (カラム名): 判定対象のカラム名
    - Operator (演算子): >, <, in, == など
    - Value (値): 比較する値
    
    この関数は、指定された入力データセット(A)に対して、条件データセット(B)のルールを適用し、
    各グループごとのフラグカラム(1または0)を作成するPrepareレシピ(shaker)を作成・更新します。
    
    Args:
        input_dataset_name (str): 編集対象となるデータセット名 (Dataset A)
        condition_dataset_name (str): 検索条件が格納されたデータセット名 (Dataset B)
        output_dataset_name (str): 出力データセット名
        recipe_name (str): 作成(または更新)するレシピ名
    """
    
    client = dataiku.api_client()
    project = client.get_default_project()
    
    # 1. 条件データセットの読み込み
    print(f"Loading conditions from {condition_dataset_name}...")
    condition_ds = dataiku.Dataset(condition_dataset_name)
    try:
        condition_df = condition_ds.get_dataframe()
    except Exception as e:
        raise ValueError(f"Failed to load condition dataset: {e}")
        
    # カラム名の正規化 (日本語カラム名に対応)
    col_map = {
        "グループ": "group",
        "カラム名": "column", 
        "演算子": "operator",
        "値": "value",
        "Group": "group",
        "Column": "column",
        "Operator": "operator",
        "Value": "value"
    }
    # 既存のカラム名をマップ
    new_cols = {}
    for col in condition_df.columns:
        if col in col_map:
            new_cols[col] = col_map[col]
            
    condition_df = condition_df.rename(columns=new_cols)
    
    # 必須カラムチェック
    required_keys = ["group", "column", "operator", "value"]
    missing = [k for k in required_keys if k not in condition_df.columns]
    if missing:
        # マップできなかった場合、元のカラム名が英語の可能性もあるのでチェックしたが、それでもなければエラー
        raise ValueError(f"Condition dataset is missing required columns (group, column, operator, value). Found: {list(condition_df.columns)}. Missing: {missing}")

    # 2. レシピの作成 (存在しない場合)
    # 出力データセットの作成が必要な場合もあるが、ここではレシピ作成時にDataikuがハンドリングすることを期待
    # もしくは事前に作成されている前提とする
    
    try:
        recipe = project.get_recipe(recipe_name)
        print(f"Recipe {recipe_name} already exists. Updating settings.")
    except:
        print(f"Creating new recipe {recipe_name}...")
        # Prepareレシピ (shaker) の作成
        # input/outputのリンクを作成
        creation_settings = {
            "type": "shaker",
            "name": recipe_name,
            "inputs": {
                "main": {
                    "items": [{"ref": input_dataset_name, "deps": []}]
                }
            },
            "outputs": {
                "main": {
                    "items": [{"ref": output_dataset_name, "appendMode": False}]
                }
            }
        }
        recipe = project.create_recipe(creation_settings)

    # 3. レシピ設定(Steps)の構築
    steps = []
    
    # グループごとに処理 (AND条件で結合)
    grouped = condition_df.groupby("group")
    
    for group_name, group_df in grouped:
        conditions = []
        for _, row in group_df.iterrows():
            col = row["column"]
            op = str(row["operator"]).strip()
            val = row["value"]
            
            cond_expr = _build_dataiku_formula(col, op, val)
            conditions.append(cond_expr)
            
        # 全条件をAND (&&) で結合
        full_formula = " && ".join(conditions)
        
        # if(cond, 1, 0)
        final_expression = f"if({full_formula}, 1, 0)"
        
        # Prepare Step (Processor) の作成
        # CreateColumnWithGREL (Formula)
        step = {
            "preview": False,
            "metaType": "PROCESSOR",
            "disabled": False,
            "type": "CreateColumnWithGREL",
            "params": {
                "column": group_name,
                "expression": final_expression,
                "custom": False
            }
        }
        steps.append(step)
        
    # 4. レシピ設定の保存
    settings = recipe.get_settings()
    
    # JSONペイロードを取得して steps を更新
    # Prepareレシピの場合、payload内の 'steps' キーにリストを格納する
    payload = settings.get_json_payload()
    payload["steps"] = steps
    settings.set_json_payload(payload)
    
    settings.save()
    print(f"Successfully updated recipe {recipe_name} with {len(steps)} group rules.")


def _build_dataiku_formula(col, op, val):
    """
    演算子と値に応じたDataiku Formula式を作成する
    """
    op = op.lower()
    val_str = str(val).strip()
    
    # 'in' 演算子 (例: "a,b,c")
    if op == "in":
        items = [x.strip() for x in val_str.split(",")]
        # val("col") == "item1" || val("col") == "item2" ...
        # 値は文字列として扱う (例に基づき)
        checks = [f'val("{col}") == "{item}"' for item in items]
        return f"({' || '.join(checks)})"
    
    # 比較演算子 (>, <, >=, <=)
    if op in [">", "<", ">=", "<="]:
        # 値が数値かどうか確認せず、そのまま式に入れる (Dataikuが型解釈する)
        # ただし、カラム参照は val("col")
        return f'val("{col}") {op} {val_str}'
    
    # 等価 (==, =)
    if op in ["==", "="]:
        # 文字列比較として安全に倒すか、数値か？
        # 例が "1" (引用符付き) だが、 df['col'] > 1 もある。
        # ユーザーの意図に合わせてクォートの有無を切り替えるのは難しい。
        # シンプルに: 数値に見えるなら数値、そうでなければダブルクォート
        if _is_number(val_str):
            return f'val("{col}") == {val_str}'
        else:
            return f'val("{col}") == "{val_str}"'
            
    # 不等価 (!=)
    if op == "!=":
        if _is_number(val_str):
            return f'val("{col}") != {val_str}'
        else:
            return f'val("{col}") != "{val_str}"'

    # デフォルト (未知の演算子)
    return f'val("{col}") {op} "{val_str}"'

def _is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
