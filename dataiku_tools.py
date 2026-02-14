import dataiku
import pandas as pd

def fast_copy(source_dataset, output_dataset, processing_funcs, chunk_size=10000):
    """
    Dataikuのdatasetを他のdatasetに処理をしながらコピーする。
    メモリ利用率を抑えるため、チャンクごとに処理を行う。
    
    Args:
        source_dataset (dataiku.Dataset): コピー元のdataset
        output_dataset (dataiku.Dataset): コピー先のdataset
        processing_funcs (list): コピー時に適用する関数のリスト。各関数はDataFrameを受け取り、DataFrameを返す。
        chunk_size (int, optional): チャンク件数。デフォルトは10,000。
    """
    
    # 最初のチャンクかどうかを判定するフラグ
    is_first_chunk = True
    
    # データセットへの書き込み用ライター（初回書き込み時に初期化）
    writer = None
    
    try:
        # チャンクごとにデータを読み込む
        for df in source_dataset.iter_dataframes(chunksize=chunk_size):
            
            # 関数のチェーン適用
            # 受け取った関数を順番に適用してDataFrameを加工する
            current_df = df
            for func in processing_funcs:
                current_df = func(current_df)
            
            # 加工後のDataFrameが空の場合はスキップ（必要に応じて処理を変更可能）
            if current_df is None:
                continue
                
            if is_first_chunk:
                # 初回チャンクの場合、出力データセットをクリアし、スキーマを設定する
                # コピー処理なので、既存データは消去する（上書き動作）
                output_dataset.clear()
                output_dataset.write_schema_from_dataframe(current_df)
                
                # ライターを初期化
                writer = output_dataset.get_writer()
                is_first_chunk = False

            
            # チャンクを書き込む
            if writer:
                writer.write_dataframe(current_df)
                
    finally:
        # ライターが開かれている場合は閉じる
        if writer:
            writer.close()

