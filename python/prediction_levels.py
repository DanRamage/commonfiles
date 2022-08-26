from wq_prediction_tests import predictionLevels
class prediction_levels(predictionLevels):
  def __str__(self):
    if self.value >= self.LOW and self.value < self.HIGH:
      return "LOW"
    elif self.value == self.HIGH:
      return "HIGH"
    elif self.value == self.DISABLED:
      return "TEST DISABLED"
    else:
      return "NO TEST"

