
import sys
#sys.path.append('../commonfiles')
from math import pow
from math import isnan as math_isnan
import logging.config
from sympy.parsing.sympy_parser import parse_expr
from sympy.core.compatibility import exec_
from sympy import Float as symFloat
from sympy import *

from collections import OrderedDict
from wqHistoricalData import wq_defines
from wq_prediction_tests import predictionTest, predictionLevels
import time

class TEST_FUNC(Function):
  nargs = 4

  def _eval_evalf(self, nprec):
      obs_symbol, a, b, c= symbols('obs_symbol a b c')
      test_func = a + b * obs_symbol + c * obs_symbol**2
      result = test_func.evalf(subs={obs_symbol: symFloat(self.args[0]), a: symFloat(self.args[1]), b: symFloat(self.args[2]), c: symFloat(self.args[3])})
      return result
class VB_POLY(Function):
  nargs = (1, 4)

  """
  @classmethod
  def _should_evalf(cls, arg):
    if arg.is_zero:
      return arg._prec
    else:
      if arg.is_Float:
          return arg._prec
      if not arg.is_Add:
          return -1
      re, im = arg.as_real_imag()
      l = [a._prec for a in [re, im] if a.is_Float]
      l.append(-1)
      return max(l)
  """
  """
  @classmethod
  def eval(cls, obs_val, a, b, c):
    if cls._should_evalf(obs_val) != -1:
      result = poly(a + b * obs_val + c * obs_val**2)
    return None
  """
  def _eval_evalf(self, nprec):
    #bs_val = 0
    obs_symbol,a,b,c = symbols('obs_symbol a b c')
    result = poly(a + b * obs_symbol + c * obs_symbol ** 2)\
      .eval({obs_symbol: symFloat(self.args[0]), a: symFloat(self.args[1]), b: symFloat(self.args[2]), c: symFloat(self.args[3])})
    #poly_func = poly(a + b * obs_symbol + c * obs_symbol**2)
    #if self.args[0] != 0:
    #  obs_val = symFloat(self.args[0])
    #result = poly_func.evalf(subs={obs_symbol: symFloat(self.args[0]), a: symFloat(self.args[1]), b: symFloat(self.args[2]), c: symFloat(self.args[3])})
    #If the obs value is 0, then no need to calc the polynomial, the value is going
    #to be the value of "a" since "b" and "c" are zeroed out.
    return result

class VB_SQUARE(Function):
  nargs = 1

  def _eval_evalf(self, nprec):
    obs = symbols('obs_symbol')
    vb_func = sign(obs) * (abs(obs)**2)
    result = vb_func.evalf(subs={obs: symFloat(self.args[0])})
    return result

class VB_QUADROOT(Function):
  nargs = 1

  def _eval_evalf(self, nprec):
    obs = symbols('obs_symbol')
    vb_func = sign(obs) * (abs(obs)**.25)
    result = vb_func.evalf(subs={obs: symFloat(self.args[0])})
    return result

class VB_SQUAREROOT(Function):
  nargs = 1

  def _eval_evalf(self, nprec):
    obs = symbols('obs_symbol')
    vb_func = sign(obs) * (abs(obs)**.5)
    result = vb_func.evalf(subs={obs: symFloat(self.args[0])})
    return result

class VB_INVERSE(Function):
  nargs = 2

  def _eval_evalf(self, nprec):
    obs = symbols('obs_symbol')
    if self.args[0] != 0:
      sub_val = symFloat(self.args[0])
    else:
      sub_val = symFloat(self.args[1])
    vb_func =  sign(obs) * (1 / abs(obs))
    result = vb_func.evalf(subs={obs: sub_val})
    return result

class VB_WindO_comp(Function):
  nargs = (1, 3)

  def _eval_evalf(self, nprec):
    wind_dir, wind_spd, beach_orientation = symbols('wind_dir wind_spd beach_orientation')
    vb_func =  wind_spd * sin((wind_dir - beach_orientation) * pi / 180)
    result = vb_func.evalf(subs={wind_dir: symFloat(self.args[0]),
                                 wind_spd: symFloat(self.args[1]),
                                 beach_orientation: symFloat(self.args[2])})
    return result

class VB_WindA_comp(Function):
  nargs = (1, 3)

  def _eval_evalf(self, nprec):
    wind_dir, wind_spd, beach_orientation = symbols('wind_dir wind_spd beach_orientation')
    vb_func =  -wind_spd * cos((wind_dir - beach_orientation) * pi / 180)
    result = vb_func.evalf(subs={wind_dir: symFloat(self.args[0]),
                                 wind_spd: symFloat(self.args[1]),
                                 beach_orientation: symFloat(self.args[2])})
    return result

class VB_LOG10(Function):
  nargs = 1
  def _eval_evalf(self, nprec):
    obs = symbols('obs_symbol')
    vb_func = log(obs, 10)
    result = vb_func.evalf(subs={obs: symFloat(self.args[0])})
    return result


"""
def VB_POLY(obs_symbol, a_val, b_val, c_val):
  a,b,c = symbols('a b c')
  poly_func = poly(a + b * obs_symbol + c * obs_symbol**2)
  poly_func.subs({a: symFloat(a_val), b: symFloat(b_val), c: symFloat(c_val)})
  return poly_func
"""
"""
Class: mlrPredictionTest
Purpose: Prediction test for a linear regression formula.
"""
class EnterococcusPredictionTest(predictionTest):
  """
  Function: __init__
  Purpose: Initialize the object.
  Parameters:
  formula - a string with the appropriate string substitution parameters that the runTest function will
    apply the data against.
  site_name - String specifying which site the test is for.
  _model_name - String giving the name of the model.
  model_enabled - Flag to specify if the model is to be tested.
  Return:
  """
  #def __init__(self, formula, site_name, _model_name, model_enabled):
  def __init__(self, **kwargs):
    predictionTest.__init__(self, kwargs.get('formula', ''), kwargs.get('model_name', ''), kwargs.get('site_name', ''), kwargs.get('model_enabled', True))
    self._test_type = "Linear Regression Equation"
    self.lowCategoryLimit = kwargs.get('low_limit', 104.0)
    self.highCategoryLimit = kwargs.get('high_limit', 500.0)
    self._mlrResult = None
    self._log10MLRResult = None
    self.logger = None
    self._data_used = OrderedDict()
    self.test_time = 0
    self.logger = logging.getLogger(type(self).__name__)

  @property
  def mlrResult(self):
    return(self._mlrResult)

  """
  Function: setCategoryLimits
  Purpose: To catecorize MLR results, we use a high and low limit.
  Parameters:
    lowLimit - Float representing the value, equal to or below, which is considered a low prediction.
    highLimit  - Float representing the value, greater than,  which is considered a high prediction.
  """
  def set_category_limits(self, lowLimit, highLimit):
    self.lowCategoryLimit = lowLimit
    self.highCategoryLimit = highLimit
  """
  Function: runTest
  Purpose: Uses the data parameter to do the string substitutions then evaluate the formula.
    Prediction is a log10 formula.
  Parameters:
    data - a dictionary with the appropriate keys to do the string subs.
  Return:
    The result of evaluating the formula.
  """
  def runTest(self, data):
    if self.enabled:
      if self.logger:
        self.logger.debug("runTest start Site: %s model name: %s formula: %s" % (self.name, self._model_name, self.formula))

      start_time = time.time()
      try:
        #Get the variables from the formula, then verify the passed in data has the observation and a valid value.
        valid_data = True
        self._result = None
        sym_expr = sympify(self.formula, globals())

        observation_variables = sym_expr.free_symbols
        mlr_symbols = {}
        for obs_var in observation_variables:
          self.data_used[obs_var.name] = None
          if obs_var.name in data:
            self.data_used[obs_var.name] = data[obs_var.name]
            mlr_symbols[obs_var] = symFloat(data[obs_var.name])
            if data[obs_var.name] == wq_defines.NO_DATA:
              valid_data = False
          else:
            valid_data = False
        if valid_data:
          self._log10MLRResult = sym_expr.evalf(subs=mlr_symbols)
          if self.logger:
            self.logger.debug("Model: %s Result: %f Data Used: %s" % (self._model_name, self._log10MLRResult, self.data_used))
          try:
            self._mlrResult = pow(10,self._log10MLRResult)
            self.categorize_result()
          except OverflowError as e:
            if self.logger:
              self.logger.exception(e)
          self._result = self._mlrResult
        else:
          if self.logger:
            self.logger.debug("Model: %s test not performed, one of more invalid data points: %s" % (self._model_name, self.data_used))
      except Exception as e:
        if self.logger:
          self.logger.exception(e)

      self.test_time = time.time() - start_time
      if self.logger:
        self.logger.debug("Test: %s execute in: %f ms" % (self._model_name, self.test_time * 1000))

        self.logger.debug("runTest finished model: %s Prediction Level: %s" % (self._model_name, self._predictionLevel))
    else:
      self.logger.debug("Test: %s is not enabled" % (self._model_name))
      self._predictionLevel.value = predictionLevels.DISABLED
    return self._predictionLevel.value

  """
  Function: mlrCategorize
  Purpose: For the regression formula, this catergorizes the value.
  Parameters:
    None
  Return:
    A predictionLevels value.
  """
  def categorize_result(self):
    if self.enabled:
      self._predictionLevel.value = predictionLevels.NO_TEST
      if self._mlrResult is not None:
        if self._mlrResult < self.lowCategoryLimit:
          self._predictionLevel.value = predictionLevels.LOW
        elif self._mlrResult >= self.highCategoryLimit:
          self._predictionLevel.value = predictionLevels.HIGH
        else:
          self._predictionLevel.value = predictionLevels.LOW
          #self.predictionLevel.value = predictionLevels.MEDIUM
    else:
      self._predictionLevel.value = predictionLevels.DISABLED

  """
  Function: getResults
  Purpose: Returns a dictionary with the variables that went into the predictionLevel.
  Parameters:
    None
  Return: A dictionary.
  """
  def get_result(self):
    name = "%s_%s_Prediction" % (self.name, self._model_name)
    results = {
               name : self._predictionLevel.__str__(),
               'log10MLRResult' : self._log10MLRResult,
               'mlrResult' : self._mlrResult
    }
    return(results)


class EnterococcusPredictionTestEx(EnterococcusPredictionTest):
  """
  Function: runTest
  Purpose: Uses the data parameter to do the string substitutions then evaluate the formula.
    Prediction is a log10 formula.
  Parameters:
    data - a dictionary with the appropriate keys to do the string subs.
  Return:
    The result of evaluating the formula.
  """
  def runTest(self, data):
    if self.enabled:
      if self.logger:
        self.logger.debug("runTest start Site: %s model name: %s formula: %s" % (self.name, self._model_name, self.formula))

      start_time = time.time()
      try:
        #Get the variables from the formula, then verify the passed in data has the observation and a valid value.      valid_data = True
        valid_data = True
        self._result = None

        sym_expr = sympify(self.formula, globals())

        observation_variables = sym_expr.free_symbols
        mlr_symbols = {}
        for obs_var in observation_variables:
          self._data_used[obs_var.name] = None
          if obs_var.name in data:
            self._data_used[obs_var.name] = data[obs_var.name]
            if data[obs_var.name] != 0:
              mlr_symbols[obs_var] = symFloat(data[obs_var.name])
            else:
              mlr_symbols[obs_var] = int(data[obs_var.name])
            if data[obs_var.name] == wq_defines.NO_DATA:
              valid_data = False
          else:
            valid_data = False
        if valid_data:
          try:
            self._mlrResult = sym_expr.evalf(subs=mlr_symbols, n=4)
            self._mlrResult = int(self.mlrResult + 0.5)
            if self._mlrResult < 0:
              self._mlrResult = 0
              self.logger.debug("Model: %s negative, resetting to 0" % (self._model_name))
            if self.logger:
              self.logger.debug("Model: %s Result: %f Data Used: %s" % (self._model_name, self._mlrResult, self._data_used))
            self.categorize_result()
          except (TypeError, OverflowError) as e:
            if self.logger:
              self.logger.exception(e)
              try:
                if math_isnan(self._mlrResult):
                  self._mlrResult = None
              except Exception as e:
                self.logger.exception(e)
                self._mlrResult = None
          self.result = self._mlrResult
        else:
          if self.logger:
            self.logger.debug("Model: %s test not performed, one of more invalid data points: %s" % (self._model_name, self._data_used))
      except Exception as e:
        if self.logger:
          self.logger.exception(e)

      self.test_time = time.time() - start_time
      if self.logger:
        self.logger.debug("Test: %s execute in: %f ms" % (self._model_name, self.test_time * 1000))

        self.logger.debug("runTest finished model: %s Prediction Level: %s" % (self._model_name, self._predictionLevel))
    else:
      self.logger.debug("Test: %s not enabled." % (self._model_name))
      self._predictionLevel.value = predictionLevels.DISABLED
    return self._predictionLevel.value
