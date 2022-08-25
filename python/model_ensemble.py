import logging.config
from wq_prediction_tests import wqEquations
from wq_prediction_tests import predictionLevels


logging = logging.getLogger()


class model_ensemble(wqEquations):
    def __init__(self, station, model_list):
        super().__init__(station, model_list)
        self._ensemblePrediction = predictionLevels(predictionLevels.NO_TEST)
        self._testing_object_categories = {}

        for model in model_list:
            self.categorize_model(model)

    def __len__(self):
        return len(self.tests)
    @property
    def test_categories(self):
        return self._testing_object_categories.keys()
    @property
    def testing_object_categories(self):
        return self._testing_object_categories

    @property
    def models(self):
        return self.tests

    def categorize_model(self, test_obj):
        if len(test_obj.test_type) and test_obj.test_type not in self._testing_object_categories:
            self._testing_object_categories[test_obj.test_type] = []
        self._testing_object_categories[test_obj.test_type].append(test_obj)

    def add_test(self, test_obj):
        self.categorize_model(test_obj)
        self._tests.append(test_obj)

    def runTests(self, test_data):
        self.data = test_data.copy()

        for testObj in self.tests:
            testObj.runTest(test_data)

        self.overall_prediction()

    def overall_prediction(self):
        '''
        Purpose: From the models used, determine the model type and come up with overall prediction level. Some models
        are binary, so either Pass/Fail, some are linear.
        :param self:
        :return:     A predictionLevels value.

        '''
        executedtests = 0
        if len(self.tests):
          sum = 0
          for model in self.tests:
            if model.predictionLevel.value == predictionLevels.LOW or\
                model.predictionLevel.value == predictionLevels.HIGH:
                sum += model.predictionLevel.value
                executedtests += 1

          if executedtests:
            self._ensemblePrediction.value = int(round(sum / executedtests))


        self.logger.debug("Overall Prediction: %d(%s)" %(self._ensemblePrediction.value, self._ensemblePrediction.__str__()))
        return self._ensemblePrediction
