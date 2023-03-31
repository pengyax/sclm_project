import yagmail


def mail(contents,subject,receiver,attaches,cc):
    yag = yagmail.SMTP(user = "pengyaxiong@saselomo.com", password='W6sC39JJ4vM3BEzx', host = 'smtp.exmail.qq.com')
    
    yag.send(receiver, subject, contents,attachments = attaches,cc=cc)
    yag.close()
    print("发送成功")

def main():
    contents = ["这是一封测试邮件",
                "换行测试"]
    subject = "测试发送邮件"
    # 发送邮件 yag.send("收件人",邮件标题,邮件内容)
    attach1 = "./ERPTrans线下4月.mdb"
    attach2 = "./202105/线下2021401.zip"
    attaches = [attach1,attach2]
    receiver = ["pengyaxiong@saselomo.com","95468419@qq.com"]
    cc = ["95468419@qq.com"]
    mail(contents,subject,receiver,attaches,cc)

if __name__ == '__main__':
    main()