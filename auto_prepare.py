import dataiku
import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import LabelEncoder, OrdinalEncoder, OneHotEncoder
from sklearn.feature_selection import mutual_info_regression
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
import gc

def auto_prepare(input_dataset_name, output_dataset_name, target_column,
                 impute_strategy='mean', encoding_strategy='label',
                 r2_threshold=0.05, mi_threshold=0.01,
                 sampling_limit=100000, chunk_size=50000):
    """
    Dataikuデータセットの前処理を自動化する関数。
    サンプリングしたデータを用いて前処理ロジック（欠損補完、エンコーディング、特徴量選択）を決定し、全量データに対しチャンク処理で適用する。
    
    Args:
        input_dataset_name (str): 入力Dataikuデータセット名
        output_dataset_name (str): 出力Dataikuデータセット名
        target_column (str): 目的変数カラム名
        impute_strategy (str): 数値カラムの欠損値補完戦略 ('mean', 'median', 'most_frequent', 'constant')。デフォルトは'mean'。
        encoding_strategy (str): カテゴリカラムのエンコーディング戦略 ('label', 'onehot')。デフォルトは'label'。
        r2_threshold (float): 特徴量選択に使用する決定係数の閾値
        mi_threshold (float): 特徴量選択に使用する相互情報量の閾値 (R2が低い場合に確認)
        sampling_limit (int): ロジック決定に使用するサンプリング行数
        chunk_size (int): 全量処理時のチャンクサイズ（行数）
    """
    
    # ---------------------------------------------------------
    # 1. サンプルデータの読み込みと分析 (Fitting Phase)
    # ---------------------------------------------------------
    print(f"Dataset '{input_dataset_name}' からサンプルデータを読み込んでいます (上限: {sampling_limit}行)...")
    input_dataset = dataiku.Dataset(input_dataset_name)
    
    # Dataikuのサンプリング機能を使用してDataFrameを取得
    df_sample = input_dataset.get_dataframe(sampling='head', limit=sampling_limit)
    
    if target_column not in df_sample.columns:
        raise ValueError(f"目的変数 '{target_column}' がデータセットに見つかりません。")
    
    # 特徴量と目的変数の分離 (サンプル)
    X_sample = df_sample.drop(columns=[target_column])
    y_sample = df_sample[target_column]
    
    # カラムタイプの特定
    numeric_cols = X_sample.select_dtypes(include=[np.number]).columns.tolist()
    categorical_cols = X_sample.select_dtypes(include=['object', 'category']).columns.tolist()
    
    print(f"数値カラム数: {len(numeric_cols)}")
    print(f"カテゴリカルカラム数: {len(categorical_cols)}")
    
    # (1) 数値項目の欠損値補完 (Fitting)
    imputer = None
    if numeric_cols:
        print(f"数値項目の欠損値補完戦略を学習しています (strategy='{impute_strategy}')...")
        imputer = SimpleImputer(strategy=impute_strategy)
        imputer.fit(X_sample[numeric_cols])
        # 特徴量選択用
        X_sample_num = pd.DataFrame(imputer.transform(X_sample[numeric_cols]), 
                                    columns=numeric_cols, index=X_sample.index)
    else:
        X_sample_num = pd.DataFrame(index=X_sample.index)
    
    # (2) カテゴリエンコーディング (Fitting)
    encoder = None
    encoded_feature_names = []
    X_sample_cat_encoded = pd.DataFrame(index=X_sample.index)
    
    if categorical_cols:
        print(f"カテゴリ項目のエンコーディング戦略を学習しています (strategy='{encoding_strategy}')...")
        
        # 欠損値対応: 文字列として埋めておく (Fitting用)
        X_sample[categorical_cols] = X_sample[categorical_cols].fillna("Missing")

        if encoding_strategy == 'label':
            # 未知のカテゴリは -1 にエンコードする設定
            encoder = OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1)
            encoder.fit(X_sample[categorical_cols])
            
            encoded_feature_names = categorical_cols
            X_sample_cat_encoded = pd.DataFrame(encoder.transform(X_sample[categorical_cols]),
                                                columns=encoded_feature_names,
                                                index=X_sample.index)
                
        elif encoding_strategy == 'onehot':
            # handle_unknown='ignore' で未知カテゴリはオール0になる
            # sparse=False で密行列として扱う（メモリ注意だがFeature Selection用に必要）
            encoder = OneHotEncoder(handle_unknown='ignore', sparse=False)
            encoder.fit(X_sample[categorical_cols])
            
            encoded_feature_names = encoder.get_feature_names_out(categorical_cols)
            X_sample_cat_encoded = pd.DataFrame(encoder.transform(X_sample[categorical_cols]),
                                                columns=encoded_feature_names,
                                                index=X_sample.index)
        else:
            raise ValueError(f"不明なエンコーディング戦略です: {encoding_strategy}")

    # 特徴量選択のための結合データ (サンプル)
    X_processed_sample = pd.concat([X_sample_num, X_sample_cat_encoded], axis=1)
    
    # (3)(4) 特徴量選択
    print("特徴量選択を開始します...")
    
    # 目的変数の欠損対応 (学習用データからは除外)
    valid_indices = y_sample.notna()
    X_eval = X_processed_sample.loc[valid_indices]
    y_eval = y_sample.loc[valid_indices]
    
    if X_eval.empty:
        raise ValueError("サンプルデータ内に有効な目的変数が存在しません。")

    if not np.issubdtype(y_eval.dtype, np.number):
        print("目的変数が数値ではありません。エンコーディングして評価します。")
        le_y = LabelEncoder()
        y_encoded = le_y.fit_transform(y_eval.astype(str))
    else:
        y_encoded = y_eval.values

    selected_features = []
    
    for col in X_eval.columns:
        feature_values = X_eval[col].values.reshape(-1, 1)
        
        # (3) 決定係数 (R2) の算出
        model = LinearRegression()
        model.fit(feature_values, y_encoded)
        predictions = model.predict(feature_values)
        r2 = r2_score(y_encoded, predictions)
        
        keep = False
        
        if r2 >= r2_threshold:
            keep = True
        else:
            # (4) 相互情報量 (MI) の算出
            mi = mutual_info_regression(feature_values, y_encoded, random_state=42)[0]
            if mi >= mi_threshold:
                keep = True
        
        if keep:
            selected_features.append(col)
        
    print(f"特徴量選択完了: 全{len(X_eval.columns)}カラム中、{len(selected_features)}カラムが選択されました。")
    
    if not selected_features:
        print("警告: 閾値を満たす特徴量がありません。処理を中断します。")
        return

    # メモリ解放
    del df_sample, X_sample, X_sample_num, X_sample_cat_encoded, X_processed_sample, X_eval, y_eval
    gc.collect()

    # ---------------------------------------------------------
    # (5) 全量データへの適用と出力 (Transform Phase with Chunking)
    # ---------------------------------------------------------
    print(f"全量データをチャンク処理で変換し、'{output_dataset_name}' に出力します (Chunk size: {chunk_size})...")
    
    output_dataset = dataiku.Dataset(output_dataset_name)
    
    # Writerを取得してチャンクごとに書き込み
    with output_dataset.get_writer() as writer:
        
        chunk_count = 0
        for df_chunk in input_dataset.iter_dataframes(chunksize=chunk_size):
            chunk_count += 1
            # print(f"Processing chunk {chunk_count}...") # ログが多すぎる場合はコメントアウト
            
            # 目的変数の確保
            y_chunk = df_chunk[target_column]
            X_chunk = df_chunk.drop(columns=[target_column])
            
            # 1. 数値補完の適用
            X_chunk_num = pd.DataFrame(index=X_chunk.index)
            if numeric_cols and imputer:
                X_chunk_num = pd.DataFrame(imputer.transform(X_chunk[numeric_cols]), 
                                          columns=numeric_cols, index=X_chunk.index)
            
            # 2. カテゴリエンコーディングの適用
            X_chunk_cat_encoded = pd.DataFrame(index=X_chunk.index)
            if categorical_cols and encoder:
                # 欠損値処理
                X_chunk[categorical_cols] = X_chunk[categorical_cols].fillna("Missing")
                
                if encoding_strategy == 'label':
                    X_chunk_cat_encoded = pd.DataFrame(encoder.transform(X_chunk[categorical_cols]),
                                                      columns=categorical_cols,
                                                      index=X_chunk.index)
                elif encoding_strategy == 'onehot':
                    X_chunk_cat_encoded = pd.DataFrame(encoder.transform(X_chunk[categorical_cols]),
                                                      columns=encoded_feature_names,
                                                      index=X_chunk.index)
            
            # チャンク内で結合
            X_chunk_processed = pd.concat([X_chunk_num, X_chunk_cat_encoded], axis=1)
            
            # 選択された特徴量のみ抽出
            X_chunk_final = X_chunk_processed[selected_features].copy()
            
            # 目的変数を戻す
            X_chunk_final[target_column] = y_chunk
            
            # 書き込み
            writer.write_dataframe(X_chunk_final)
            
    print("完了しました。")

if __name__ == '__main__':
    # テスト用
    pass
