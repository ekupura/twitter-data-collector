import reg
import MeCab
import pprint
import pymysql

class PartOfSpeechExtractor(reg.Regex):
    # override
    def replace(self, string):
        def requirement(pos):
            p = pos.split('-')
            main_type = p[0]
            sub_type = None if len(p) == 1 else p[1]
            if main_type == '動詞' and sub_type == '自立':
                return True
            elif main_type == '形容詞' and sub_type == '自立':
                return True
            elif main_type == '名詞':
                if sub_type == '特殊':
                    return False
                elif sub_type == '代名詞':
                    return False
                elif sub_type == '接尾':
                    return False
                elif sub_type == '非自立':
                    return False
                else:
                    return True
            else:
                return False

        tagger = MeCab.Tagger('-Ochasen -d /usr/lib/mecab/dic/mecab-ipadic-neologd')
        output_string = ''
        parsed_string = tagger.parse(string)
        for s in parsed_string.split('\n'):
            keywords = s.split("\t")
            if keywords[0] == 'EOS':
                return output_string.strip()
            part_of_speech = keywords[3]
            if requirement(part_of_speech):
                output_string += keywords[2] + ' '

if __name__ == '__main__':
    e = PartOfSpeechExtractor()
    e.run("random_tweets_removed_noise","random_tweets_wakachied")
