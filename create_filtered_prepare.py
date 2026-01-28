import dataiku
import dataikuapi
import json

def create_filtered_prepare_recipe(input_dataset_name, output_dataset_name, target_tag, connection_name="filesystem_managed"):
    """
    指定されたタグを持つカラムのみを抽出するPrepareレシピと出力データセットを動的に作成する関数。

    Args:
        input_dataset_name (str): インプットとなるデータセット名
        output_dataset_name (str): アウトプットとなるデータセット名
        target_tag (str): 抽出対象とするカラムのタグ
        connection_name (str): 出力データセットを作成するコネクション名 (デフォルト: filesystem_managed)
    """
    
    # クライアントとプロジェクトの取得
    client = dataiku.api_client()
    project = client.get_default_project()
    
    # 1. インプットデータセットの情報を取得し、対象カラムを特定する
    input_dataset = project.get_dataset(input_dataset_name)
    schema = input_dataset.get_schema()
    columns = schema.get('columns', [])
    
    # 指定されたタグを持つカラム名をリストアップ
    target_columns = []
    all_columns = []
    
    for col in columns:
        col_name = col.get('name')
        all_columns.append(col_name)
        
        # カラムのタグを確認 (tagsキーが存在する場合)
        # 注意: Dataikuのバージョンによってはカラムレベルのタグがサポートされていない、
        # または取得方法が異なる場合がありますが、標準的なスキーマ構造を想定しています。
        col_tags = col.get('tags', []) 
        
        if target_tag in col_tags:
            target_columns.append(col_name)
            
    print(f"全カラム数: {len(all_columns)}")
    print(f"抽出対象カラム数 ('{target_tag}'タグ保持): {len(target_columns)}")
    
    if not target_columns:
        print("警告: 指定されたタグを持つカラムが見つかりませんでした。処理を中断します。")
        return

    # 2. アウトプットデータセットの作成 (存在しない場合)
    # 既に存在する場合は取得、無ければ作成
    try:
        output_dataset = project.get_dataset(output_dataset_name)
        output_dataset.get_settings() # 存在確認のため設定を取得
        print(f"データセット '{output_dataset_name}' は既に存在します。")
    except:
        print(f"データセット '{output_dataset_name}' を作成します。")
        # マネージドデータセットとして作成
        builder = project.new_managed_dataset(output_dataset_name)
        builder.with_store_into(connection_name)
        builder.create()
    
    # 3. Prepareレシピの作成
    recipe_name = f"compute_{output_dataset_name}"
    
    # レシピが既に存在するか確認して削除または取得 (ここでは再作成のロジックとする)
    try:
        recipe = project.get_recipe(recipe_name)
        print(f"レシピ '{recipe_name}' は既に存在するため、設定を更新します。")
    except:
        print(f"Prepareレシピ '{recipe_name}' を作成します。")
        # Prepareレシピ ('shaker') の作成
        builder = project.new_recipe("shaker", recipe_name)
        builder.with_input(input_dataset_name)
        builder.with_output(output_dataset_name)
        recipe = builder.create()

    # 4. レシピの設定 (スクリプト) を更新して、特定カラムのみを残す
    # Prepareレシピでは「削除しないカラム」を指定する機能が直接または「他を削除」で実現可能
    # ここでは「残すべきカラム以外を削除する」ステップを追加する手法をとる
    
    columns_to_remove = list(set(all_columns) - set(target_columns))
    
    # Prepareレシピのスクリプト定義
    script = {
        "steps": [
            {
                "metaType": "PROCESSOR",
                "preview": False,
                "disabled": False,
                "name": "不要なカラムの削除",
                "alwaysShowComment": False,
                "comment": f"タグ '{target_tag}' を持たないカラムを削除",
                "type": "DeleteColumn",
                "params": {
                    "columns": columns_to_remove,
                    "style": "MULTI"  # 複数カラム指定モード
                }
            }
        ],
        "maxProcessedMemTableBytes": -1,
        "columnsSelection": {
            "mode": "ALL"
        },
        "columnWidthsByName": {},
        "coloring": {
            "scheme": "MEANING_AND_STATUS",
            "individualColumns": [],
            "valueColoringMode": "HASH"
        },
        "sorting": [],
        "analysisColumnData": {},
        "explorationSampling": {
            "selection": {
                "maxStoredBytes": 104857600,
                "timeout": -1,
                "filter": {
                    "distinct": False,
                    "enabled": False
                },
                "partitionSelectionMethod": "ALL",
                "latestPartitionsN": 1,
                "ordering": {
                    "enabled": False,
                    "rules": []
                },
                "samplingMethod": "HEAD_SEQUENTIAL",
                "maxRecords": 10000,
                "targetRatio": 0.02,
                "withinFirstN": -1,
                "maxReadUncompressedBytes": -1
            },
            "autoRefreshSample": True,
            "_refreshTrigger": 0
        },
        "vizSampling": {
            "selection": {
                "useMemTable": False,
                "filter": {
                    "distinct": False,
                    "enabled": False
                },
                "partitionSelectionMethod": "ALL",
                "latestPartitionsN": 1,
                "ordering": {
                    "enabled": False,
                    "rules": []
                },
                "samplingMethod": "FULL",
                "maxRecords": -1,
                "targetRatio": 0.02,
                "withinFirstN": -1,
                "maxReadUncompressedBytes": -1
            },
            "autoRefreshSample": False,
            "_refreshTrigger": 0
        }
    }

    # 設定の保存
    # Prepareレシピの設定等は Payload に格納されている
    settings = recipe.get_settings()
    
    # 既存のペイロードを取得して、script部分を上書きする方法もあるが、
    # ここでは新規作成に近いのでペイロード全体またはscriptステップを設定する
    
    # 注: dataikuapiのバージョンによってAPIが若干異なる場合があるため、汎用的なset_payloadを使用しない場合もあるが
    # 一般的には settings.get_payload() でJSON文字列を取得し、編集して set_payload() する
    
    # PrepareレシピのペイロードはJSON文字列
    settings.set_payload(json.dumps(script))
    settings.save()
    
    print(f"レシピ '{recipe_name}' の作成と設定が完了しました。")
    print(f"インプット: {input_dataset_name}")
    print(f"アウトプット: {output_dataset_name}")
    print(f"残したカラム数: {len(target_columns)}")

if __name__ == '__main__':
    # 使用例
    # 実際のデータセット名とタグに合わせて変更してください
    INPUT_DATASET = "input_data"
    OUTPUT_DATASET = "filtered_data"
    TAG_TO_KEEP = "important"
    
    print("--- 処理開始 ---")
    try:
        create_filtered_prepare_recipe(INPUT_DATASET, OUTPUT_DATASET, TAG_TO_KEEP)
    except Exception as e:
        print(f"エラーが発生しました: {e}")
