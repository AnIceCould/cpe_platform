# 导入必要的库
from concurrent import futures
import time
import grpc
import pickle
import pandas as pd
import numpy as np

# 导入从 .proto 文件生成的代码
import prediction_pb2
import prediction_pb2_grpc

# --- 模型加载 ---
# 在服务器启动时，只加载一次模型
MODEL_PATH = 'controller_model_xgb.pkl'
MODEL = None

try:
    with open(MODEL_PATH, 'rb') as f:
        MODEL = pickle.load(f)
    print(f"✅ 模型 '{MODEL_PATH}' 加载成功！")
except FileNotFoundError:
    print(f"❌ 错误: 找不到模型文件 '{MODEL_PATH}'。请将模型文件放置在与此脚本相同的目录下。")
    exit()
except Exception as e:
    print(f"❌ 加载模型时发生未知错误: {e}")
    exit()


# 定义服务实现类
class PredictionServiceImpl(prediction_pb2_grpc.PredictionServiceServicer):
    """
    实现了 gRPC 服务接口，并使用加载的 XGBoost 模型进行预测。
    """

    def PredictPacketLoss(self, request, context):
        """
        接收 RTT 列表，使用 XGBoost 模型进行预测，并返回结果。
        """
        # 1. 从请求中获取 RTT 列表
        rtts = list(request.rtts)
        print(f"📬 接收到 gRPC 请求，RTT 列表: {rtts}")

        if MODEL is None:
            print("🛑 错误: 模型未加载，无法进行预测。")
            # 在 gRPC 中，可以通过 context 返回错误状态
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details('模型未成功加载，服务器无法处理预测请求。')
            return prediction_pb2.PacketLossResponse()

        # 2. 将输入数据转换为模型可以理解的格式 (Pandas DataFrame)
        # 【【【非常重要】】】
        # 这里的列名必须与您训练模型时使用的特征列名完全一致。
        # 我们假设模型是基于5个RTT特征进行训练的。如果您的特征名不同，请修改这里的列名列表。
        feature_names = [f'rtt_{i+1}' for i in range(len(rtts))]
        input_df = pd.DataFrame([rtts], columns=feature_names)
        
        print(f"📊 正在为模型准备输入数据 (DataFrame):\n{input_df}")

        # 3. 使用加载的模型进行预测
        try:
            # model.predict() 返回一个 NumPy 数组, e.g., [0] or [1]
            prediction_array = MODEL.predict(input_df)
            # 提取第一个预测结果
            prediction = prediction_array[0]
            
            # 将 0 或 1 的预测结果转换为布尔值
            prediction_result = bool(prediction == 1)
            
            print(f"🧠 模型预测结果: {prediction} => {'可能丢包' if prediction_result else '正常'}")

        except Exception as e:
            print(f"🛑 模型预测时发生错误: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'模型预测时发生错误: {e}')
            return prediction_pb2.PacketLossResponse()

        # 4. 创建并返回响应
        response = prediction_pb2.PacketLossResponse(hasPacketLoss=prediction_result)
        print(f"🚀 正在返回响应: {response.hasPacketLoss}")
        print("-----------------------------------------")
        
        return response

def serve():
    """
    启动 gRPC 服务器。
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    prediction_pb2_grpc.add_PredictionServiceServicer_to_server(
        PredictionServiceImpl(), server
    )
    server.add_insecure_port('[::]:9090')
    server.start()
    print("✅ gRPC 预测服务器已启动 (使用XGBoost模型)，正在监听端口 9090...")
    
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print("🛑 服务器正在关闭...")
        server.stop(0)

if __name__ == '__main__':
    serve()