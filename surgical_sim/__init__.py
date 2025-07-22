import itertools
import logging
from collections import Counter, defaultdict, namedtuple
from dataclasses import dataclass, field
from typing import List

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from sim_tools.distributions import Exponential

from .patients import (Patient, elective_generator, emergency_generator,
                       initial_elective_generator, initial_emergency_generator)
from .processing import daily_planning, scheduler
from .resources import surgery
from .schedule import Schedule, slot

SEED = 42

EMERGENCY_MEAN_IAT = 10
EMERGENCY_SURGICAL_DURATION = 3
EMERGENCY_MEAN_RECOVERY_TIME = 60

ELECTIVE_MEAN_IAT = 16
ELECTIVE_SURGICAL_DURATION = 2
ELECTIVE_MEAN_RECOVERY_TIME = 48

RUN_LENGTH = 480
NUM_BEDS = 10
NUM_CC_BEDS = 2

DAILY = 24
WEEKLY = 7 * DAILY

INITIAL_NUMBER_OF_ELECTIVE = 3
INITIAL_NUMBER_OF_EMERGENCY = 1

MAX_EMERGENCY_WAIT = 48


class Experiment:
    def __init__(
        self,
        seed=SEED,
        initial_number_of_elective=INITIAL_NUMBER_OF_ELECTIVE,
        initial_number_of_emergency=INITIAL_NUMBER_OF_EMERGENCY,
        elective_mean_iat=ELECTIVE_MEAN_IAT,
        emergency_mean_iat=EMERGENCY_MEAN_IAT,
        elective_surgical_duration=ELECTIVE_SURGICAL_DURATION,
        emergency_surgical_duration=EMERGENCY_SURGICAL_DURATION,
        elective_mean_recovery_time=ELECTIVE_MEAN_RECOVERY_TIME,
        emergency_mean_recovery_time=EMERGENCY_MEAN_RECOVERY_TIME,
        max_emergency_wait=MAX_EMERGENCY_WAIT,
    ):
        self.patients = []

        self.initial_number_of_elective = initial_number_of_elective
        self.initial_number_of_emergency = initial_number_of_emergency

        seeds = np.random.SeedSequence(seed).spawn(6)

        self.emergency_arrival_dist = Exponential(
            emergency_mean_iat, random_seed=seeds[0]
        )
        self.elective_arrival_dist = Exponential(
            elective_mean_iat, random_seed=seeds[1]
        )

        self.elective_surgical_duration_dist = Exponential(
            elective_surgical_duration, random_seed=seeds[2]
        )
        self.emergency_surgical_duration_dist = Exponential(
            emergency_surgical_duration, random_seed=seeds[3]
        )

        self.elective_recovery_time_dist = Exponential(
            elective_mean_recovery_time, random_seed=seeds[4]
        )
        self.emergency_recovery_time_dist = Exponential(
            emergency_mean_recovery_time, random_seed=seeds[5]
        )

        self.max_emergency_wait = max_emergency_wait


####### Logging config
class SimTimeFilter:
    """
    A logging filter that injects the current simulation time into log records.

    Attributes:
        env (simpy.Environment): The simulation environment used to retrieve the current time.
    """

    def __init__(self, env):
        """
        Initializes the filter with a SimPy environment.

        Args:
            env (simpy.Environment): The simulation environment.
        """
        self.env = env

    def filter(self, record):
        """
        Adds the current simulation time to the log record.

        Args:
            record (logging.LogRecord): The log record to modify.

        Returns:
            bool: Always returns True to allow the record to be logged.
        """
        record.sim_time = self.env.now
        return True


def setup_logger(env, level=logging.INFO):
    """
    Configures the root logger to include simulation time in log messages.

    Args:
        env (simpy.Environment): The simulation environment used to track time.
        level (int, optional): Logging level (e.g., logging.INFO, logging.DEBUG). Defaults to logging.INFO.

    Returns:
        logging.Logger: The configured logger instance.
    """
    logger = logging.getLogger()
    logger.setLevel(level)

    # Looks like iPython already starts with handlers...
    logger.handlers.clear()

    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)s - %(sim_time).2f - %(message)s")

    handler.setFormatter(formatter)

    logger.addHandler(handler)
    # Abuse the filter to add new information to the log record (sim_time)
    logger.addFilter(SimTimeFilter(env))

    return logger
