import pyspark.sql.functions as psf
import pyspark.sql import Row
from pyspark.sql.functions import avg, udf, col
from operator import add
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.types import *
import sys
import os

class sqlJoin(object):
	def __init__(self,):
		self.sourceRdd = None
		self.desRdd = None
		self.joinT = None
		self.result = None
		return
	def set_source_table(self,rdd = None):
		if rdd is not None:
			self.sourceRdd = rdd.alias("sourceRdd")
		return
	def set_des_table(self, rdd = None):
		if rdd is not None:
			self.desRdd = rdd.alias("desRdd")
		return
	def set_join_type(self, joinT  = 'leftanti'):
		if joinT is not None:
			self.joinT = joinT
		return
	def set_param_list(self, param = []):
		self.paramL = param
		if self.paramL is not None:
			for ind, each in enumerate(self.paramL):
				self.paramL[ind] = psf.col(self.sourceRdd + "." + each) == psf.col(self.desRdd + "." + each)  
		return
	def perform_join(self, joinT = 'leftanti', *paramList):
		if len(paramList) > 0:
			paramList[0] = ",".join(paramList[0]) 
		
		self.result = self.sourceRdd.join(self.desJoin, self.paramL, self.joinT) if self.joinT is 'leftanti' else self.sourceRdd.join(self.desJoin, self.paramL, self.joinT).select(paramList[0]) if len(paramList) < 2 else self.sourceRdd.join(self.desJoin, self.paramL, self.joinT).select(paramList[0], paramList[1])
	
	def find_by_key(self, rdd, paramName = None, dataType = None):
		try:
			if paramName is None:
				raise ValueError
		except ValueError, e:
			print "Param can not be None"
			print e
			return sqlContext.createDataFrame(sc.emptyRDD(), StructType([]))
		if dataType is not None:
			filteredRdd = filteredRdd.withColumn(paramName,filteredRdd[paramName].cast(dataType))
			
		filteredRdd = rdd.alias("rdd").filter(psf.col("rdd." + colName).isin( set(paramName)))
			
		return filteredRdd
	def column_type_cast(self, rdd = None, colName = None, datatype = None):
		try:
			if datatype is None or colName is None:
				raise ValueError
		except ValueError, e:
			print " the datatype is None"
			print "operation exiting without any performing"
			print e
			return rdd
		if rdd.columns.count(colName) is 0:
			print "Column name is missing from rdd"
			return rdd
		rdd = rdd.withColumn(colName, rdd[colName].cast(datatype))
		return rdd
	def add_remove_column(self, rdd = None, colName  = None, opT = 0, _typeValue = None):
		try:
			if colName is None :
				raise ValueError
		except ValueError, e:
			print "Column name is None"
			print "Exiting ..."
			return  rdd 
		if opT is 0:
			rdd = rdd.drop(colName) 
		if opT is 1 :
			try:
				if _typeValue is None:
					raise ValueError
			except ValueError, e:
				print "Value of new column can not be  None"
				print "Default value is None"
				print "Not adding the columns"
				print e
				return rdd
			rdd = rdd.withColumn(colName, lit(_typeValue))
		return rdd
	def get_results(self,):
		return self.result
	@staticmethod	
	def factoryFun(sourceRdd = None, desRdd = None, joinT = None, paramList = None):
		this = sqlJoin()
		this.set_source_table(rdd = sourceRdd)
		this.set_des_table(rdd = desRdd)
		this.set_join_type(joinT = joinT)
		this.set_param_list(param = paramList)
		return this
