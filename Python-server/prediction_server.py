# 导入所有必要的库
from concurrent import futures
import time
import grpc
import pickle
import pandas as pd
import numpy as np
import os
import warnings
from pathlib import Path

# 导入从 .proto 文件生成的代码
import prediction_pb2
import prediction_pb2_grpc

warnings.filterwarnings("ignore")

# --- 1. 数据和模型加载函数 (完全来自您的脚本) ---

BASE_DIR = Path("./example")
SAVE_DIR1 = BASE_DIR
MODEL_PATH = 'controller_model_xgb.pkl' # 假设模型在根目录

def load_file(filepath):
    """
    从指定路径加载并合并所有 cpe_a...-mobile.csv 文件。
    """
    all_dfs = []
    header_saved = None

    print(f"开始从 '{filepath}' 加载训练数据...")
    if not os.path.isdir(filepath):
        print(f"❌ 错误: 找不到训练数据目录 '{filepath}'。请检查文件夹是否存在。")
        return None, None

    for filename in os.listdir(filepath):
        if filename.endswith('-mobile.csv') and filename.startswith('cpe_a'):
            file_path = os.path.join(filepath, filename)
            try:
                df = pd.read_csv(file_path)
                if header_saved is None:
                    header_saved = df.columns
                
                df_no_header = pd.read_csv(file_path, skiprows=1, header=None)
                all_dfs.append(df_no_header)
            except Exception as e:
                print(f"加载文件 {filename} 时出错: {e}")

    if not all_dfs or header_saved is None:
        print(f"❌ 错误: 在 '{filepath}' 中没有找到任何有效的训练数据文件。")
        return None, None

    merged_df = pd.concat(all_dfs, ignore_index=True)
    merged_df.columns = header_saved

    y = merged_df["label"]
    # 确保丢弃的列与您的脚本完全一致
    X = merged_df.drop(columns=["label", "std_delay", "loss_ratio", "last_delay"])
    
    print(f"✅ 成功加载 {len(merged_df)} 条训练数据。")
    return X, y

# --- 2. 在服务器启动时，加载数据并训练模型 ---

# 加载训练数据
X_train, y_train = load_file(SAVE_DIR1)

# 加载未经训练的模型结构
try:
    with open(MODEL_PATH, 'rb') as f:
        MODEL = pickle.load(f)
    print(f"✅ 模型结构 '{MODEL_PATH}' 加载成功！")
except FileNotFoundError:
    print(f"❌ 错误: 找不到模型文件 '{MODEL_PATH}'。")
    MODEL = None
except Exception as e:
    print(f"❌ 加载模型时发生未知错误: {e}")
    MODEL = None

# 【【【核心修正】】】
# 如果数据和模型都加载成功，则立即进行训练
if X_train is not None and y_train is not None and MODEL is not None:
    print("⏳ 正在使用加载的数据训练模型 (fitting)...")
    try:
        MODEL.fit(X_train, y_train)
        print("✅ 模型训练完成，服务已准备就绪！")
    except Exception as e:
        print(f"❌ 模型训练时发生错误: {e}")
        MODEL = None # 将模型置为None，防止后续预测出错
else:
    print("🛑 因数据或模型加载失败，无法训练模型。服务器将无法进行预测。")
    MODEL = None


# --- 3. gRPC 服务实现 (此部分逻辑不变) ---

class PredictionServiceImpl(prediction_pb2_grpc.PredictionServiceServicer):
    def PredictPacketLoss(self, request, context):
        print(f"📬 接收到 gRPC 请求...")

        if MODEL is None:
            print("🛑 错误: 模型未准备好，无法进行预测。")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details('模型未成功训练，服务器无法处理预测请求。')
            return prediction_pb2.PacketLossResponse()

        features = {
            'mean_delay': request.mean_delay,
            'min_delay': request.min_delay,
            'mid_delay': request.mid_delay,
            'max_delay': request.max_delay,
            'slope_delay': request.slope_delay,
            'mean_of_last_three': request.mean_of_last_three,
            'diff_between_last_two': request.diff_between_last_two,
            'range': request.range,
            'delay_1': request.delay_1,
            'delay_2': request.delay_2,
            'delay_3': request.delay_3,
            'delay_4': request.delay_4,
            'delay_5': request.delay_5,
        }
        
        column_order = [
            'mean_delay', 'min_delay', 'mid_delay', 'max_delay', 'slope_delay',
            'mean_of_last_three', 'diff_between_last_two', 'range',
            'delay_1', 'delay_2', 'delay_3', 'delay_4', 'delay_5'
        ]
        
        input_df = pd.DataFrame([features]).reindex(columns=column_order)
        print(f"📊 正在为模型准备输入数据 (DataFrame):\n{input_df.to_string()}")

        try:
            # 【【【新增】】】 记录预测开始时间
            start_time = time.time()

            prediction_array = MODEL.predict(input_df)
            
            # 【【【新增】】】 记录预测结束时间并计算耗时
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000

            prediction = prediction_array[0]
            prediction_result = bool(prediction == 1)
            print(f"🧠 模型预测结果: {prediction} => {'可能丢包' if prediction_result else '正常'}")
            
            # 【【【新增】】】 在控制台打印耗时
            print(f"⏱️ 本次预测耗时: {duration_ms:.2f} 毫秒")

        except Exception as e:
            print(f"🛑 模型预测时发生错误: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'模型预测时发生错误: {e}')
            return prediction_pb2.PacketLossResponse()

        response = prediction_pb2.PacketLossResponse(hasPacketLoss=prediction_result)
        print(f"🚀 正在返回响应: {response.hasPacketLoss}")
        print("-----------------------------------------")
        
        return response

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    prediction_pb2_grpc.add_PredictionServiceServicer_to_server(
        PredictionServiceImpl(), server
    )
    server.add_insecure_port('[::]:9090')
    server.start()
    print("✅ gRPC 预测服务器已启动，正在监听端口 9090...")
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()

