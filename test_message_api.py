#!/usr/bin/env python3

from rocketmq.client import Message

# 创建一个 Message 对象
msg = Message("test_topic")

print("Message 对象的可用属性:")
for attr in dir(msg):
    if not attr.startswith('_'):
        print(f"  {attr}")

# 检查一些可能的属性名
possible_attrs = ['body', 'message_body', 'content', 'data', 'payload', 'get_body', 'get_message_body', 'get_body', 'get_message_body', 'get_property', 'get_tags', 'get_keys']
print("\n检查可能的属性:")
for attr in possible_attrs:
    if hasattr(msg, attr):
        print(f"✓ 找到 {attr}")
    else:
        print(f"✗ 没有找到 {attr}")

# 尝试调用 set_body 然后检查是否有对应的 get 方法
print("\n尝试设置 body 后检查:")
msg.set_body(b"test message")
for attr in dir(msg):
    if not attr.startswith('_') and 'body' in attr.lower():
        print(f"  {attr}") 