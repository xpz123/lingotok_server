def on_message(msg_ptr, context_ptr):
    """消息回调函数 - 兼容旧版本 RocketMQ 客户端"""
    try:
        print(f"收到消息指针: {msg_ptr}, 上下文指针: {context_ptr}")
        
        
        print("消息处理成功")
        return True
        
    except Exception as e:
        print(f"处理消息时出错: {e}")
        import traceback
        traceback.print_exc()
        return False
