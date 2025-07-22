import itertools
import logging
from collections import defaultdict, namedtuple

import numpy as np
import pandas as pd


class Schedule:
    """
    A class to manage and allocate surgery slots for patients based on repeatable time windows.

    Attributes:
        lcm (int): Least common multiple of all slot repeat periods.
        processed_schedule (pd.DataFrame): A DataFrame representing the expanded and structured schedule.
    """

    def __init__(self, slots):
        """
        Initializes the Schedule with a list of slot objects.

        Args:
            slots (List[Any]): A list of slot objects, each with attributes `start_time`, `end_time`, `repeat_period`, and `patient_type`.
        """
        self.__schedule = defaultdict(lambda: [])
        self.lcm = None

        self.__process_slots(slots)

        self.processed_schedule = self.__process_schedule(self.__schedule)

    def __process_slots(self, slots):
        """
        Processes the input slots to build a time-indexed schedule dictionary.

        Args:
            slots (List[Any]): A list of slot objects.
        """
        repeat_periods = [slot.repeat_period for slot in slots]
        if repeat_periods != []:
            self.lcm = np.lcm.reduce(repeat_periods)
        else:
            self.lcm = 1

        for slot in slots:
            repeat_slots = itertools.repeat(
                slot.start_time, self.lcm // slot.repeat_period
            )
            for multiplicative, start_time in enumerate(repeat_slots):

                processed_time = multiplicative * slot.repeat_period + start_time

                self.__schedule[processed_time] += [
                    {slot.patient_type: slot.end_time - slot.start_time}
                ]

        self.__schedule = dict(sorted(self.__schedule.items()))

    def __getitem__(self, time):
        """
        Retrieves the schedule for a specific hour, expanding the schedule if needed.

        Args:
            time (int): The hour to retrieve the schedule for.

        Returns:
            pd.DataFrame: A DataFrame row corresponding to the specified hour.
        """
        while time > max(self.__schedule):
            multiple = max(self.__schedule) // self.lcm + 1
            later_schedule = {
                k + (self.lcm * multiple): v for k, v in self.__schedule.items()
            }
            self.__schedule |= later_schedule

            df = self.__process_schedule(later_schedule)
            self.processed_schedule = pd.concat(
                [self.processed_schedule, df]
            ).reset_index(drop=True)

        return self.processed_schedule.loc[(self.processed_schedule["hour"] == time), :]

    def __process_schedule(self, schedule):
        """
        Converts the internal schedule dictionary into a structured DataFrame.

        Args:
            schedule (Dict[int, List[Dict[str, float]]]): The internal schedule dictionary.

        Returns:
            pd.DataFrame: A structured DataFrame with columns for hour, patient type, duration, and assigned patients.
        """
        df = pd.DataFrame(
            [
                [
                    time,
                    list(slot.keys())[0],
                    list(slot.values())[0],
                    list(slot.values())[0],
                    [],
                ]
                for time, slots in schedule.items()
                for slot in slots
            ],
            columns=[
                "hour",
                "patient_type",
                "hours_total",
                "hours_remaining",
                "patients",
            ],
        )
        return df

    def schedule_patients(self, patients, time):
        """
        Assigns patients to available slots in the schedule.

        Args:
            patients (List[Any]): A list of patient objects with attributes `id` and `surgery_duration`.
            time (int): The current simulation time.
        """
        patient_types = self.processed_schedule["patient_type"].unique()
        for patient in patients:
            patient_type = patient_types[
                np.where(
                    [patient_type in patient.id for patient_type in patient_types]
                )[0]
            ][0]

            sub_df = self.__find_slot(patient_type, patient, time)

            # if nothing matches in the schedule currently
            if len(sub_df) == 0:
                max_val = max(self.__schedule)
                self[max_val + 1]

                sub_df = self.__find_slot(patient_type, patient, time)

                if len(sub_df) == 0:
                    raise ValueError(
                        f"Surgery duration for patient {patient.id} is too long for any surgery slot with a duration of {patient.surgery_duration}."
                    )

            row_mask = self.processed_schedule.index == sub_df.index[0]
            self.processed_schedule.loc[
                row_mask, "hours_remaining"
            ] -= patient.surgery_duration
            self.processed_schedule.loc[row_mask, "patients"].values[0].append(patient)

    def __find_slot(self, patient_type, patient, time):
        """
        Finds a suitable slot for a patient based on type and duration.

        Args:
            patient_type (str): The type of patient.
            patient (Any): The patient object.
            time (int): The current simulation time.

        Returns:
            pd.DataFrame: A filtered DataFrame of matching slots.
        """

        return self.processed_schedule.loc[
            (self.processed_schedule["patient_type"] == patient_type)
            & (self.processed_schedule["hours_remaining"] > patient.surgery_duration)
            & (time < self.processed_schedule["hour"])
        ]

    def find_patient(self, patient):
        """
        Finds the schedule entry for a specific patient.

        Args:
            patient (Any): The patient object.

        Returns:
            pd.DataFrame: A DataFrame row where the patient is scheduled.
        """
        return self.processed_schedule.loc[
            self.processed_schedule["patients"].apply(lambda r: patient in r), :
        ]

    def cancel_patient(self, patient):
        """
        Cancels a patient’s scheduled surgery by removing them from the slot.

        Args:
            patient (Any): The patient object to cancel.
        """
        p_df = self.find_patient(patient).patients.to_list()[0]

        idx = p_df.index(patient)
        p_df.pop(idx)


slot = namedtuple(
    "surgery_slot", ["start_time", "end_time", "patient_type", "repeat_period"]
)
