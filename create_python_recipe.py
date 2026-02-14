import pandas as pd
import dataikuapi
from faker import Faker
import sys
import os

# Dataiku接続設定 (環境に合わせて変更するか、引数で渡してください)
DEFAULT_HOST = "http://localhost:11200"
DEFAULT_API_KEY = "YOUR_API_KEY"

def get_client(host, api_key):
    """Dataikuのクライアントを取得する"""
    return dataikuapi.DSSClient(host, api_key)

def create_dataset_with_dummy_data(project, name, schema_sheet_name, excel_file):
    """データセットを作成し、スキーマ設定とダミーデータを投入する"""
    # 既存チェック
    for dataset in project.list_datasets():
        if dataset['name'] == name:
            print(f"Dataset {name} already exists. Skipping.")
            return

    print(f"Creating dataset: {name}")
    # データセット作成 (FilesystemManaged)
    dataset = project.create_dataset(name, type='FilesystemManaged')
    
    # スキーマ読み込み
    try:
        schema_df = pd.read_excel(excel_file, sheet_name=schema_sheet_name)
    except Exception as e:
        print(f"Error reading sheet {schema_sheet_name}: {e}")
        return

    # ダミーデータ生成
    fake = Faker('jp_JP')
    num_rows = 100
    
    data_dict = {}
    dss_schema_columns = []
    
    for _, row in schema_df.iterrows():
        col_name = row['column_name']
        dtype_str = str(row['data_type']).lower()
        
        # Dataikuスキーマへのマッピング
        dss_type = 'string'
        if 'int' in dtype_str: dss_type = 'bigint'
        elif 'float' in dtype_str or 'double' in dtype_str: dss_type = 'double'
        elif 'bool' in dtype_str: dss_type = 'boolean'
        elif 'date' in dtype_str: dss_type = 'date'
        
        dss_schema_columns.append({'name': col_name, 'type': dss_type})
        
        # Fakerによるデータ生成
        vals = []
        for _ in range(num_rows):
            if 'int' in dtype_str:
                vals.append(fake.random_int(min=0, max=1000))
            elif 'float' in dtype_str or 'double' in dtype_str:
                vals.append(fake.pyfloat(left_digits=3, right_digits=2, positive=True))
            elif 'date' in dtype_str:
                 vals.append(str(fake.date()))
            elif 'bool' in dtype_str:
                vals.append(fake.boolean())
            elif 'name' in dtype_str:
                vals.append(fake.name())
            elif 'mail' in dtype_str or 'email' in dtype_str:
                vals.append(fake.email())
            else:
                vals.append(fake.word())
        data_dict[col_name] = vals

    # スキーマ設定
    dataset.set_schema({'columns': dss_schema_columns})
    
    # データのアップロード
    # FilesystemManagedの場合、外部APIからの直接書き込みは制限がある場合がありますが、
    # CSVアップロードを試みます。
    df = pd.DataFrame(data_dict)
    csv_str = df.to_csv(index=False)
    
    try:
        # アップロード形式での書き込みを試行
        dataset.uploaded_data_from_str(csv_str)
    except Exception as e:
        print(f"Warning: Could not upload data to {name}: {e}")

def create_folder(project, name):
    """マネージドフォルダを作成する"""
    for folder in project.list_managed_folders():
        if folder['name'] == name:
             print(f"Folder {name} already exists. Skipping.")
             return
    print(f"Creating folder: {name}")
    project.create_managed_folder(name)

def get_recipe_code_from_sheet(excel_file, sheet_name):
    """指定された名前のシートからコードを読み取る"""
    try:
        # シート存在確認のために全シート名を読み込むのはコストが高いが、
        # pandasのExcelFileを使えばメタデータだけ読める
        xls = pd.ExcelFile(excel_file)
        if sheet_name not in xls.sheet_names:
            return None
        
        # ヘッダーなしで読み込む (コードとみなす)
        df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
        
        code_lines = []
        for _, row in df.iterrows():
            # 行内の欠損していない値を文字列として結合
            parts = [str(val) for val in row if pd.notnull(val)]
            if parts:
                code_lines.append(" ".join(parts))
        
        return "\n".join(code_lines)
    except Exception as e:
        print(f"Info: No code sheet found or error reading sheet '{sheet_name}': {e}")
        return None

def create_recipe_logic(project, inputs, outputs, excel_file):
    """Pythonレシピを作成する"""
    # レシピ名の決定 (出力データセット名に基づく)
    if not outputs:
        recipe_name = "compute_analytics_recipe"
    else:
        recipe_name = f"compute_{outputs[0]}"
        
    # 既存チェック
    try:
        project.get_recipe(recipe_name)
        print(f"Recipe {recipe_name} already exists. Skipping creation but might update code.")
        # 既存レシピでもコード更新のためにオブジェクトを取得する処理に進むことも可能だが、
        # ここでは「作成済みなら処理を行わない」という要件に従いスキップする
        return
    except:
        pass

    print(f"Creating python recipe: {recipe_name}")
    
    # レシピ作成ビルダ
    builder = project.new_recipe("python")
    for inp in inputs:
        builder.with_input(inp)
    for out in outputs:
        builder.with_output(out)
        
    recipe = builder.create(recipe_name)
    
    # スクリプトの決定
    # mainシートのname (ここではoutputsの名前) に対応するシートがあれば、そこからコードを読む
    custom_script = None
    if outputs:
        # 出力データセット名を優先して探す
        for out_name in outputs:
            code = get_recipe_code_from_sheet(excel_file, out_name)
            if code:
                custom_script = code
                print(f"Loaded custom script from sheet: {out_name}")
                break
    
    # もし該当がなければInputsから探す（要件の解釈によるが、念のため）
    if custom_script is None and inputs:
        for inp_name in inputs:
            code = get_recipe_code_from_sheet(excel_file, inp_name)
            if code:
                custom_script = code
                print(f"Loaded custom script from sheet: {inp_name}")
                break

    if custom_script:
        script = custom_script
    else:
        # デフォルトのボイラープレート
        script = "# -*- coding: utf-8 -*-\nimport dataiku\nimport pandas as pd, numpy as np\nfrom dataiku import pandasutils as pdu\n\n"
        
        for inp in inputs:
            dataset_safe = inp.replace('.', '_') # 変数名として安全にする簡易処理
            script += f"# Read recipe inputs\n{dataset_safe} = dataiku.Dataset(\"{inp}\")\n{dataset_safe}_df = {dataset_safe}.get_dataframe()\n\n"
            
        if outputs:
            out = outputs[0]
            out_safe = out.replace('.', '_')
            input_ref = inputs[0].replace('.', '_') + "_df" if inputs else "pd.DataFrame()"
            script += f"# Write recipe outputs\n{out_safe} = dataiku.Dataset(\"{out}\")\n{out_safe}.write_with_schema({input_ref})\n"
         
    settings = recipe.get_settings()
    settings.set_payload(script)
    settings.save()

def run(excel_file, project_key, host=DEFAULT_HOST, api_key=DEFAULT_API_KEY):
    client = get_client(host, api_key)
    project = client.get_project(project_key)
    
    print(f"Processing project: {project_key} with config: {excel_file}")
    
    # mainシートの読み込み
    try:
        main_df = pd.read_excel(excel_file, sheet_name='main')
    except Exception as e:
        print(f"Failed to read 'main' sheet from {excel_file}: {e}")
        return
    
    inputs = []
    outputs = []
    
    # 各行の処理
    for _, row in main_df.iterrows():
        name = row['name']
        io_type = row['io_type']
        data_type = row['data_type']
        data_name = row['data_name']
        
        if data_type == 'dataset':
            create_dataset_with_dummy_data(project, name, data_name, excel_file)
        elif data_type == 'folder':
            create_folder(project, name)
            
        if io_type == 'input':
            inputs.append(name)
        elif io_type == 'output':
            outputs.append(name)
    
    # レシピ作成
    create_recipe_logic(project, inputs, outputs, excel_file)
    print("Done.")

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python create_python_recipe.py <project_key> <excel_file> [host] [api_key]")
    else:
        p_key = sys.argv[1]
        ex_file = sys.argv[2]
        h = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_HOST
        k = sys.argv[4] if len(sys.argv) > 4 else DEFAULT_API_KEY
        run(ex_file, p_key, h, k)
