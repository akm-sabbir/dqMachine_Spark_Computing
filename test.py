#!/usr/python -*- utf-8 -*-

import codecs
def read_from_file(file_name):
    with codecs.open(file_name, encoding='utf8') as data_reader:
        for each_line in data_reader:
            yield each_line
    return 
    
    
def main_operation():
    with codecs.open('E:\\dqMachine\\output_data.txt',encoding='utf8',mode='w+') as data_writer:
        for each in read_from_file(file_name='E:\\dqMachine\\Verb_list'):
            data = each.split('<')[0].split(':')
            data_writer.write(data[0]+'\n')
            
    return
    
if __name__=='__main__':
    main_operation()