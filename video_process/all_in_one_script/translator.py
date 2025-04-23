import json

from volcengine.ApiInfo import ApiInfo
from volcengine.Credentials import Credentials
from volcengine.ServiceInfo import ServiceInfo
from volcengine.base.Service import Service

def translate_text2ar(text_list, target_lang):
    k_access_key = 'AKLTOTgzODg1Y2FiNDI5NGE3Mzk3MWEzYzJlODE3MDk2MzQ' # https://console.volcengine.com/iam/keymanage/
    k_secret_key = 'TTJJM016azRaR0V3WXpRMk5EUXhPR0kyT0RBNVlUY3hZVGd5WlRrMlpHTQ=='
    k_service_info = \
        ServiceInfo('translate.volcengineapi.com',
                    {'Content-Type': 'application/json'},
                    Credentials(k_access_key, k_secret_key, 'translate', 'cn-north-1'),
                    5,
                    5)
    k_query = {
        'Action': 'TranslateText',
        'Version': '2020-06-01'
    }
    k_api_info = {
        'translate': ApiInfo('POST', '/', k_query, {}, {})
    }
    service = Service(k_service_info, k_api_info)

    def chunk_list(lst, chunk_size):
        return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

    chunk_text_list = chunk_list(text_list, 15)
    translated_text_list = []
    for item in chunk_text_list:
        body = {
            'TargetLanguage': target_lang,
            'TextList': item,
        }
        res = service.json('translate', {}, json.dumps(body))
        translated_text_list += json.loads(res)["TranslationList"]

    return translated_text_list

if __name__ == "__main__":
    text_list = ['我爸是离了婚才过来追的你', '堂堂正正把你娶进家门', '怎么就见不得人呀', '这么多年过去了', '我奶奶连门都不让我们进', '还口口声声骂你是小三', '凭什么呀', '爸嗯妈', '我敬你们一杯', '祝你们来年嗯', '平平安安', '快快乐乐', '事事顺心', '尤其是老爸', '多赚银子少喝酒', '听到没有', '嗯女儿的话老爸你一定听', '来走着来', '新年快乐', '哎新年快乐', '新年快乐', '新年快乐', '哎呀今年啊', '咱们潇潇不得了', '你看自己开了公司', '还做了老总', '把公司打理得井井有条', '你这大老板', '是不是应该给小老板一点奖励啊', '嗯早就准备好了', '明天一早给', '一早给潇潇', '来', '爸爸也敬你一杯', '龙生龙凤生凤', '我生的女儿像我', '别不会就会做生意', '那是走着', '是啊我这妹妹本事可大', '多大能耐', '刚回来一年', '七拐八绕的朋友', '乱七八糟的狐朋狗友', '能绕着魔都拉一圈了', '什么事办不成啊', '是吧妹妹', '那我也比不上哥哥你呀', '你招惹的女孩都能排成一个', '排了怎么样', '操作心嫂怎么样', '好了好了', '大过年的掐什么掐', '接电话一定是奶奶的电话', '喂啊奶奶', '我就知道是您', '是啊过年了', '孙子给您拜个年', '祝您呃福寿绵长', '新春大吉', '身体健健康康的啊', '是我们明天就回去', '那您也知道嘛', '这两年情况都比较', '去吧去什么去啊', '曲家都不认识我们俩', '我还得假扮假设给他拜年', '他认不认识他的事', '你认不认他是你的事', '你看你爸就得认他', '去吧啊你等着啊', '你孙女跟你说话', '奶奶我是曲潇潇啊', '祝你新年快乐', '美国买的那个保养品没拿吧', '回来这么早啊', '不用送咱了', '爸爸过两天就回来', '那个孩子', '别人送的', '那个发酵火腿和老山参一块拿上啊', '够了够了', '平时也没少往家里送东西啊', '拿这么多妈肯定吃不了', '妈吃不吃是他的事', '我这个媳妇得做好', '要不然你妈还不定怎么说我呢', '那行了我知道你受委屈了', '我妈她她', '她就那么个臭脾气', '认定了我可以下来连接她吗', '她一个乡下女人没见识', '你何必跟她计较啊', '这么多年都这么', '过来就打声', '为了我再忍忍再忍忍', '哎呦潇潇啊', '我和你哥去两天就回', '这两天在家里好好陪着妈妈', '听到了没有', '你这个话头说吧', '要什么雪莲袄', '包包限量的', '刚看中一个俩', '好好好买买买买', '爸怎么了', '爸啊你快点', '磨磨蹭蹭的', '你们着什么急啊', '我的车比你们车快', '快点干嘛呢', '坐哪呢差劲样', '哎呀有些人机关算尽', '可是就没想到连回家的资格都没有', '不就是回趟乡下吗', '谁稀罕啊', '好走不送', '呵呵怎么了', '看你憋屈那样', '难受妈', '我爸是离了婚才过来追的你', '堂堂正正把你娶进家门', '怎么就见不得人呀', '这么多年过去了', '我奶奶连门都不让我们进', '还口口声声骂你是小三', '凭什么呀', '你爸不说了吗', '他刚到上海那几年', '都是那一边在照顾你奶奶', '后来虽然离了婚', '可你奶奶那死脑筋就是转不过弯来', '只认那边是儿媳妇', '有什么办法', '那你不能这么包子呀', '你帮我爸这么多年', '他假装没看见我这么大个孙女他', '假装没看见他说起笑', '醒来也没见他手软啊', '拿着我们家的东西去补贴那边', '什么意思', '算了他毕竟是你奶奶', '再说了大过年的', '我也不想让你爸为难你', '不去就不去呗', '真让你跟你爸回老家', '你未必待的惯', '不是吧你也玩自我安慰这一套', '这么心慈手软', '我可看不下去啊', '不说了不说了', '越说越烦', '不吃了回房间去', '哎徐潇潇', '你干什么呢', '嗨安迪姐', '你在干什么呀', '安迪姐', '你去那个普吉岛好不好玩啊', '我听说那海可漂亮了', '我都不知道我什么时候才能去', '邱莹莹你是猪啊', '别吃了过年不减肥', '年后徒伤悲', '可千万别听樊大姐的', '她有王帅哥', '你可什么都没', '普吉岛有什么好玩的', '要去就去大溪地', '那才叫一个美呢', '徐小胖', '你说为什么人家过年家里热热闹闹', '我们家就这么冷清', '问你呢说话呀', '不是吧那七门九溜', '大过年的连句新年问候都没有']
    ar_text_list = translate_text2ar(text_list, "ar")
    assert len(text_list) == len(ar_text_list)