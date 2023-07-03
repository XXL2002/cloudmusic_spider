import openpyxl


# 将json格式的值列表转换为逗号分割的字符串
def json2str(data):

    return ','.join([str(i) for i in list(data.values())])


# 将json格式值数据写入csv文件
def save_csv(path, data):
    
    with open(path, 'a', encoding='utf-8') as f:
        f.write(json2str(data) + "\n")
        
    f.close()
    


def create_xlsx(path, header):

    # 创建工作簿
    workbook = openpyxl.Workbook()

    # 获取表
    sheet = workbook.active

    # 设置表头
    sheet.append(header)
    
    # 保存文件
    workbook.save(path)

    return sheet