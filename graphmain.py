import streamlit as st
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from pyvis.network import Network
import json
import streamlit.components.v1 as components
import random
import base64
from io import BytesIO

# ページ設定
st.set_page_config(layout="wide", page_title="グラフ可視化ツール")
st.title("グラフ構造の可視化ツール")

# サイドバーの設定
st.sidebar.header("設定")

# サンプルデータを生成する関数
def generate_sample_data():
    # ノードデータ
    nodes = [
        {"id": 1, "label": "ノード1", "group": "グループA", "size": 25, "description": "これはノード1の説明です"},
        {"id": 2, "label": "ノード2", "group": "グループA", "size": 20, "description": "これはノード2の説明です"},
        {"id": 3, "label": "ノード3", "group": "グループB", "size": 30, "description": "これはノード3の説明です"},
        {"id": 4, "label": "ノード4", "group": "グループB", "size": 15, "description": "これはノード4の説明です"},
        {"id": 5, "label": "ノード5", "group": "グループC", "size": 25, "description": "これはノード5の説明です"},
    ]
    
    # エッジデータ
    edges = [
        {"source": 1, "target": 2, "weight": 1, "relationship": "関連"},
        {"source": 1, "target": 3, "weight": 2, "relationship": "依存"},
        {"source": 2, "target": 4, "weight": 1, "relationship": "参照"},
        {"source": 3, "target": 5, "weight": 3, "relationship": "包含"},
        {"source": 4, "target": 5, "weight": 1, "relationship": "関連"},
    ]
    
    return pd.DataFrame(nodes), pd.DataFrame(edges)

# データのアップロードまたはサンプルデータの生成
data_option = st.sidebar.radio(
    "データソース",
    ["サンプルデータを使用", "CSVファイルをアップロード"]
)

if data_option == "CSVファイルをアップロード":
    st.sidebar.write("## ノードデータのアップロード")
    st.sidebar.write("CSV形式で、少なくとも 'id', 'label' 列が必要です。")
    nodes_file = st.sidebar.file_uploader("ノードCSVファイル", type=["csv"])
    
    st.sidebar.write("## エッジデータのアップロード")
    st.sidebar.write("CSV形式で、少なくとも 'source', 'target' 列が必要です。")
    edges_file = st.sidebar.file_uploader("エッジCSVファイル", type=["csv"])
    
    if nodes_file and edges_file:
        nodes_df = pd.read_csv(nodes_file)
        edges_df = pd.read_csv(edges_file)
    else:
        st.sidebar.write("CSVファイルがアップロードされていないため、サンプルデータを使用します。")
        nodes_df, edges_df = generate_sample_data()
else:
    nodes_df, edges_df = generate_sample_data()

# 必須列の確認
required_node_cols = ["id", "label"]
required_edge_cols = ["source", "target"]

if not all(col in nodes_df.columns for col in required_node_cols):
    st.error(f"ノードデータには少なくとも {', '.join(required_node_cols)} 列が必要です。")
    st.stop()

if not all(col in edges_df.columns for col in required_edge_cols):
    st.error(f"エッジデータには少なくとも {', '.join(required_edge_cols)} 列が必要です。")
    st.stop()

# グラフの表示設定
st.sidebar.write("## グラフ表示設定")
show_labels = st.sidebar.checkbox("ノードラベルを表示", value=True)
node_size_option = st.sidebar.selectbox(
    "ノードサイズ基準",
    ["一律", *[col for col in nodes_df.columns if col != "id" and pd.api.types.is_numeric_dtype(nodes_df[col])]]
)

# グラフの構築
def build_graph(nodes_df, edges_df):
    G = nx.Graph()
    
    # ノードを追加
    for _, row in nodes_df.iterrows():
        node_id = row['id']
        node_attrs = row.to_dict()
        G.add_node(node_id, **node_attrs)
    
    # エッジを追加
    for _, row in edges_df.iterrows():
        source = row['source']
        target = row['target']
        edge_attrs = {k: v for k, v in row.items() if k not in ['source', 'target']}
        G.add_edge(source, target, **edge_attrs)
    
    return G

# Pyvisを使用してインタラクティブなグラフを作成
def create_pyvis_graph(G, show_labels, node_size_option):
    net = Network(notebook=True, height="600px", width="100%", bgcolor="#222222", font_color="white")
    
    # グループごとに色を割り当て
    group_colors = {}
    if "group" in nodes_df.columns:
        groups = nodes_df["group"].unique()
        colors = ["#FF5733", "#33FF57", "#3357FF", "#FF33A8", "#33FFF5", "#F5FF33", "#FF33F5"]
        for i, group in enumerate(groups):
            group_colors[group] = colors[i % len(colors)]
    
    # ノードを追加
    for node_id in G.nodes():
        node_attrs = G.nodes[node_id]
        label = node_attrs.get("label", str(node_id)) if show_labels else ""
        
        # ノードサイズを決定
        if node_size_option == "一律":
            size = 25
        else:
            size = node_attrs.get(node_size_option, 25)
            # サイズの正規化（最小10、最大50）
            max_size = nodes_df[node_size_option].max()
            min_size = nodes_df[node_size_option].min()
            if max_size != min_size:
                size = 10 + 40 * (size - min_size) / (max_size - min_size)
            else:
                size = 25
        
        # ノードの色を決定
        if "group" in node_attrs and node_attrs["group"] in group_colors:
            color = group_colors[node_attrs["group"]]
        else:
            color = "#1F78B4"
        
        # ノードの追加
        net.add_node(
            node_id, 
            label=label, 
            title=f"ID: {node_id}<br>Label: {node_attrs.get('label', '')}", 
            size=size, 
            color=color
        )
    
    # エッジを追加
    for source, target, attrs in G.edges(data=True):
        weight = attrs.get("weight", 1)
        title = "<br>".join([f"{k}: {v}" for k, v in attrs.items()])
        net.add_edge(source, target, value=weight, title=title)
    
    # グラフの物理的設定
    net.repulsion(node_distance=100, spring_length=200)
    net.toggle_physics(True)
    
    return net

# グラフを構築
G = build_graph(nodes_df, edges_df)
net = create_pyvis_graph(G, show_labels, node_size_option)

# HTMLファイルに保存して表示
html_file = "graph.html"
net.save_graph(html_file)
with open(html_file, "r", encoding="utf-8") as f:
    html = f.read()

# データの詳細を表示するセクション
st.write("## グラフ可視化")

# 2列レイアウト
col1, col2 = st.columns([2, 1])

with col1:
    # グラフの表示
    components.html(html, height=600)

with col2:
    # ノード詳細タブとエッジリストタブ
    tab1, tab2 = st.tabs(["ノード詳細", "エッジリスト"])
    
    # ノード詳細タブ
    with tab1:
        st.write("### ノード情報")
        st.write("ノードをクリックすると、ここにノードの詳細情報が表示されます。")
        
        # カスタムコールバック関数を定義
        if 'selected_node' not in st.session_state:
            st.session_state.selected_node = None
        
        def handle_node_click():
            if st.session_state.node_selectbox:
                st.session_state.selected_node = int(st.session_state.node_selectbox)
        
        # 選択ボックスを作成してノード選択をシミュレート
        node_options = [(str(node), f"{node} - {G.nodes[node].get('label', '')}") for node in G.nodes()]
        node_keys = [opt[0] for opt in node_options]
        node_labels = [opt[1] for opt in node_options]
        
        st.selectbox(
            "ノードを選択",
            options=node_keys,
            format_func=lambda x: node_labels[node_keys.index(x)] if x in node_keys else x,
            key="node_selectbox",
            on_change=handle_node_click
        )
        
        # 選択されたノードの情報を表示
        selected_node = st.session_state.get('selected_node', None)
        if selected_node and selected_node in G.nodes:
            node_attrs = G.nodes[selected_node]
            st.write(f"**選択されたノード:** {node_attrs.get('label', selected_node)}")
            
            # ノード属性テーブル
            attrs_df = pd.DataFrame([node_attrs]).T.reset_index()
            attrs_df.columns = ['属性', '値']
            st.dataframe(attrs_df, use_container_width=True)
            
            # 関連するエッジの表示
            st.write("**関連するエッジ:**")
            connected_edges = []
            for u, v, data in G.edges(data=True):
                if u == selected_node or v == selected_node:
                    other_node = v if u == selected_node else u
                    other_label = G.nodes[other_node].get('label', other_node)
                    direction = "発信" if u == selected_node else "受信"
                    edge_info = {
                        "接続先ノード": other_label,
                        "方向": direction,
                    }
                    edge_info.update(data)
                    connected_edges.append(edge_info)
            
            if connected_edges:
                connected_edges_df = pd.DataFrame(connected_edges)
                st.dataframe(connected_edges_df, use_container_width=True)
            else:
                st.write("接続されているエッジはありません。")
    
    # エッジリストタブ
    with tab2:
        st.write("### エッジリスト")
        
        # エッジテーブルを作成
        edge_list = []
        for u, v, data in G.edges(data=True):
            source_label = G.nodes[u].get('label', u)
            target_label = G.nodes[v].get('label', v)
            edge_info = {
                "始点ID": u,
                "始点ラベル": source_label,
                "終点ID": v,
                "終点ラベル": target_label,
            }
            edge_info.update(data)
            edge_list.append(edge_info)
        
        edge_df = pd.DataFrame(edge_list)
        st.dataframe(edge_df, use_container_width=True)

# グラフ分析情報
st.write("## グラフ分析")

# 基本的なグラフ情報
st.write(f"**ノード数:** {G.number_of_nodes()}")
st.write(f"**エッジ数:** {G.number_of_edges()}")

# グラフの次数分布を計算
degree_dict = dict(G.degree())
degree_df = pd.DataFrame({
    'ノードID': list(degree_dict.keys()),
    '次数': list(degree_dict.values())
})
degree_df = degree_df.sort_values('次数', ascending=False)

# 次数上位のノードを表示
st.write("### 次数上位のノード")
top_degree_nodes = degree_df.head(min(5, len(degree_df)))
top_degree_nodes['ラベル'] = top_degree_nodes['ノードID'].apply(lambda x: G.nodes[x].get('label', x))
st.dataframe(top_degree_nodes[['ノードID', 'ラベル', '次数']], use_container_width=True)

# 次数分布の可視化
st.write("### 次数分布")
fig, ax = plt.subplots(figsize=(8, 4))
ax.hist(degree_df['次数'], bins=min(10, G.number_of_nodes()), alpha=0.7)
ax.set_xlabel('次数')
ax.set_ylabel('ノード数')
ax.grid(True, alpha=0.3)
st.pyplot(fig)

# グラフデータのダウンロード機能
st.write("## グラフデータのダウンロード")

# ノードデータのダウンロード
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

nodes_csv = convert_df_to_csv(nodes_df)
st.download_button(
    label="ノードデータをダウンロード",
    data=nodes_csv,
    file_name='nodes.csv',
    mime='text/csv',
)

# エッジデータのダウンロード
edges_csv = convert_df_to_csv(edges_df)
st.download_button(
    label="エッジデータをダウンロード",
    data=edges_csv,
    file_name='edges.csv',
    mime='text/csv',
)

# PyVisネットワークにカスタムイベントを追加する試み
net.set_options("""
{
  "interaction": {
    "hover": true,
    "navigationButtons": true,
    "keyboard": true
  },
  "physics": {
    "forceAtlas2Based": {
      "gravitationalConstant": -50,
      "centralGravity": 0.01,
      "springLength": 200,
      "springConstant": 0.08
    },
    "maxVelocity": 50,
    "solver": "forceAtlas2Based",
    "timestep": 0.35,
    "stabilization": {
      "enabled": true,
      "iterations": 1000,
      "updateInterval": 25
    }
  }
}
""")