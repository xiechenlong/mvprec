#coding=utf-8
from odps.udf import annotate
from math import log

@annotate("*->bigint")
class LogBucket(object):
    def evaluate(self, pv, i, k, m):
        if pv == None or pv <= 0:
            return 0
        else:
            return min(m, int(log(k + pv, i)))

@annotate("*->bigint")
class ConversionRateBucket(object):
    def evaluate(self, click_count, exposure_count, thresholds):
        if exposure_count == None or exposure_count == 0:
            return 0
        rate = click_count / float(exposure_count + 100)  # 添加平滑操作
        for i, threshold in enumerate(thresholds):
            if rate <= threshold:
                return i
        return len(thresholds)
    
@annotate("*->bigint")
class TruncateBucket(object):
    def evaluate(self, value, k):
        if value == None:
            return 0
        else:
            return min(value, k)