import logging.config
import os.path
import time
import json
import pandas as pd
from catboost import CatBoostClassifier, CatBoostRegressor

from prediction_levels import prediction_levels
from wq_prediction_tests import predictionTest

logger = logging.getLogger()


class cbm_model_classifier(predictionTest):
    def __init__(self,
                 site_name,
                 model_name,
                 model_type,
                 model_file,
                 false_positive_threshold,
                 false_negative_threshold,
                 model_data_list,
                 missing_data_value=-9999):
        super().__init__(formula=None, model_name=model_name, site_name=site_name, enabled=True)
        self._test_type = model_type

        self._model_data_list = model_data_list
        self._test_time = 0
        self._result = None
        self._model_file = model_file
        self._false_positive_threshold = false_positive_threshold
        self._false_negative_threshold = false_negative_threshold
        self._missing_data_value = missing_data_value
        self._categorical_feature_names = []
        try:
            self._cbm_model = CatBoostClassifier()
            format = "cbm"
            if self._model_file.find('json') != -1:
                format = "json"
                #OPen the json file to see if we have categorical features.
                self.find_categorical_features(self._model_file)
            self._cbm_model.load_model(self._model_file, format=format)
        except Exception as e:
            logger.exception(e)
            raise e
        self._predicted_values = None
        self._prediction_probabilities = None
        self._predictionLevel = prediction_levels(prediction_levels.NO_TEST)
        self._X_test = None

    @property
    def result(self):
        return self._result
    @property
    def prediction_level(self):
        return self._predictionLevel

    @property
    def model_data(self):
        data_tuples = {}
        for column in self._X_test:
            data_tuples[column] = self._X_test[column][0]
        return data_tuples
    @property
    def prediction_value(self):
        return(self._predicted_values)
    @property
    def prediction_proba(self):
        return(self.prediction_proba)

    def find_categorical_features(self, model_file):
        try:
            with open(model_file, "r") as fp_model_file:
                json_model = json.load(fp_model_file)
                try:
                    if "categorical_features" in json_model["features_info"]:
                        for cat_feature in json_model["features_info"]["categorical_features"]:
                            self._categorical_feature_names.append(cat_feature)
                except Exception as e:
                    logger.error("Unable to find sections in json.")
                    logger.exception(e)
        except Exception as e:
            logger.exception(e)
        return

    def runTest(self, site_data):
        try:
            start_time = time.time()
            logger.debug("Site: %s Model: %s test" % (self._site_name, self._model_name))
            for data_param in self._model_data_list:
                dest_key = src_key = data_param
                if data_param.find('tide_') == -1:
                    dest_key = data_param
                else:
                    # For the tide data, we have 2 sets of data, one set is for the older
                    # linear regression equations, and then one set for our machine learning.
                    # We want to strip out the _pda(peek detection algorithm) substring when
                    # building the data dict.
                    pda_param = "%s_pda" % (data_param)
                    if pda_param in site_data:
                        dest_key = data_param
                        src_key = pda_param

                val = site_data[src_key]
                #If the value is the missing data value, we want to replace it with None
                #as that is what catboost understands as a missing value.
                if val == self._missing_data_value:
                    val = None
                self._data_used[dest_key] = [val]

            X_test = pd.DataFrame(self._data_used)


            self._predicted_values = self._cbm_model.predict(X_test)
            self._prediction_probabilities = self._cbm_model.predict_proba(X_test)
            if self._predicted_values[0]:
                self._predictionLevel.value = prediction_levels.HIGH
                self._result = "High"
            else:
                self._predictionLevel.value  = prediction_levels.LOW
                self._result = "Low"


            self._test_time = time.time() - start_time
        except Exception as e:
            logger.exception(e)
            self._predictionLevel.value  = prediction_levels.NO_TEST

        logger.debug("Model: %s result: %s finished in %f seconds." % (self._site_name, self._result, self.test_time))
        return

    def runTestDF(self, site_data):
        try:
            start_time = time.time()
            logger.debug("Site: %s Model: %s test" % (self._site_name, self._model_name))
            #Take the whole dataframe and create a new dataframe with just hte observations the model needs.
            model_features = self._cbm_model.feature_names_
            logger.debug(f"Features for model: {model_features}")
            self._X_test = site_data[model_features].copy()
            #If we have any categorical features, we need to change the dftype to str.
            for cat_feature in self._categorical_feature_names:
                logger.debug(f"Updating categorical feature column dtype: {cat_feature['feature_id']}")
                if cat_feature['feature_id'] in self._X_test:
                    self._X_test[cat_feature['feature_id']] = self._X_test[cat_feature['feature_id']].astype('str')
                else:
                    logger.error(f"Categorical columns {cat_feature['feature_id']} not found in dataframe")

            self._predicted_values = self._cbm_model.predict(self._X_test)
            self._prediction_probabilities = self._cbm_model.predict_proba(self._X_test)
            if self._predicted_values[0]:
                self._predictionLevel.value = prediction_levels.HIGH
                self._result = "High"
            else:
                self._predictionLevel.value = prediction_levels.LOW
                self._result = "Low"


            self._test_time = time.time() - start_time
        except Exception as e:
            logger.exception(e)
            self._predictionLevel.value  = prediction_levels.NO_TEST

        logger.debug("Model: %s result: %s finished in %f seconds." % (self._site_name, self._result, self.test_time))
        return

class cbm_model_regressor(predictionTest):
    def __init__(self,
                 site_name,
                 model_name,
                 model_type,
                 low_limit,
                 high_limit,
                 model_file,
                 model_data_list):
        super().__init__(formula=None, model_name=model_name, site_name=site_name, enabled=True)
        self._test_type = model_type

        self.low_limit = low_limit
        self.high_limit = high_limit

        self._model_data_list = model_data_list
        self._test_time = 0
        self._result = None
        self._model_file = model_file
        self._categorical_feature_names = []

        try:
            self._cbm_model = CatBoostRegressor()
            format = "cbm"
            if self._model_file.find('json') != -1:
                format = "json"
                #OPen the json file to see if we have categorical features.
                self.find_categorical_features(self._model_file)

            self._cbm_model.load_model(self._model_file, format=format)
        except Exception as e:
            logger.exception(e)
            raise e
        self._predicted_values = None
        self._prediction_probabilities = None
        self._predictionLevel = prediction_levels(prediction_levels.NO_TEST)
        self._model_data = None

    @property
    def model_type(self):
        return self._model_type
    @property
    def result(self):
        return self._result
    @property
    def model_data(self):
        return self._model_data
    @property
    def prediction_level(self):
        return self._predictionLevel

    def find_categorical_features(self, model_file):
        try:
            with open(model_file, "r") as fp_model_file:
                json_model = json.load(fp_model_file)
                try:
                    if "categorical_features" in json_model["features_info"]:
                        for cat_feature in json_model["features_info"]["categorical_features"]:
                            self._categorical_feature_names.append(cat_feature)
                except Exception as e:
                    logger.error("Unable to find sections in json.")
                    logger.exception(e)
        except Exception as e:
            logger.exception(e)
        return

    def runTestDF(self, site_data):
        try:
            start_time = time.time()
            logger.debug("Site: %s Model: %s test" % (self._site_name, self._model_name))
            model_features = self._cbm_model.feature_names_
            logger.debug(f"Features for model: {model_features}")
            self._X_test = site_data[model_features].copy()
            #If we have any categorical features, we need to change the dftype to str.
            for cat_feature in self._categorical_feature_names:
                logger.debug(f"Updating categorical feature column dtype: {cat_feature['feature_id']}")
                if cat_feature['feature_id'] in self._X_test:
                    self._X_test[cat_feature['feature_id']] = self._X_test[cat_feature['feature_id']].astype('str')
                else:
                    logger.error(f"Categorical columns {cat_feature['feature_id']} not found in dataframe")
            self._predicted_values = self._cbm_model.predict(self._X_test)
            self._result = float(self._predicted_values[0])
            if self._result >= self.high_limit:
                self._predictionLevel.value = prediction_levels.HIGH
            else:
                self._predictionLevel.value = prediction_levels.LOW

            self._test_time = time.time() - start_time
        except Exception as e:
            logger.exception(e)
            self._predictionLevel.value  = prediction_levels.NO_TEST

        logger.debug("Model: %s result: %s finished in %f seconds." % (self._site_name, self._result, self.test_time))
        return

