import itertools
import logging
import simpy
import numpy as np

from dataclasses import dataclass, field
from collections import defaultdict, namedtuple, Counter

from typing import List

from matplotlib import pyplot as plt
import pandas as pd

from sim_tools.distributions import Exponential


@dataclass
class Patient:
    id: str
    arrival_time: int = None
    surgical_time: int = None 
    discharge_time: int = None
    surgery_duration: int = None
    recovery_time: int = None
    cancellations: List = field(default_factory=lambda : [])


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
            seed = SEED, 
            initial_number_of_elective = INITIAL_NUMBER_OF_ELECTIVE,
            initial_number_of_emergency = INITIAL_NUMBER_OF_EMERGENCY,
            elective_mean_iat = ELECTIVE_MEAN_IAT,
            emergency_mean_iat = EMERGENCY_MEAN_IAT, 
            elective_surgical_duration = ELECTIVE_SURGICAL_DURATION,
            emergency_surgical_duration = EMERGENCY_SURGICAL_DURATION, 
            elective_mean_recovery_time = ELECTIVE_MEAN_RECOVERY_TIME,
            emergency_mean_recovery_time = EMERGENCY_MEAN_RECOVERY_TIME,
            max_emergency_wait = MAX_EMERGENCY_WAIT
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
    def __init__(self, env):
        self.env = env

    def filter(self, record):
        record.sim_time = self.env.now
        return True


def setup_logger(env, level = logging.INFO):
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


########### Theatre
def surgery(env, beds, cc_beds, patients, hours_available, schedule, experiment, metrics):
    end_time = env.now + hours_available

    metrics["beds"].append((env.now, beds.count))
    for patient in patients:
        # 0.01 is a hack in case the surgery duration and time remaining is equal
        timeout = env.timeout(max(0, end_time - env.now - patient.surgery_duration - 1))

        cc_bed_req = cc_beds.request()
        result = yield env.any_of([cc_bed_req, timeout])
        logging.info(f"Requesting cc bed for patient {patient}")

        if cc_bed_req in result:
            metrics["beds"].append((env.now, beds.count))

            logging.info(f"Assigned cc bed to {patient}")
            metrics["cc_bed_event"].append((env.now, 1))

            bed_req = beds.request()
            
            logging.info(f"Requesting bed for patient {patient}")
            timeout = env.timeout(max(0, end_time - env.now - patient.surgery_duration - 1))
            result = yield env.any_of([bed_req, timeout])

            if bed_req in result:
                metrics["beds"].append((env.now, beds.count))

                logging.info(f"Assigned bed to {patient}")
                metrics["bed_event"].append((env.now, 1))

                surgical_time = env.now
                metrics["surgical_event"].append((surgical_time, 1))
                
                patient.surgical_time = surgical_time

                yield env.timeout(patient.surgery_duration)
                logging.info(f"{patient.id} had surgery for: {patient.surgery_duration} hours")
                metrics["surgical_event"].append((env.now, -1))

                # TODO: random select if patient goes to CC (what to do with bed until then?)
                cc_beds.release(cc_bed_req)
                metrics["cc_bed_event"].append((env.now, -1))

                env.process(ward(env, beds, bed_req, patient, metrics))

                metrics["beds"].append((env.now, beds.count))

            else:
                logging.info(f"{patient.id} cancelled, beds at {beds.count}, hours remaining: {end_time - env.now}, surgical duration: {patient.surgery_duration}")
                schedule.schedule_patients([patient], env.now)

                cc_beds.release(cc_bed_req)
                metrics["cc_bed_event"].append((env.now, -1))

                patient.cancellations.append(env.now)

                bed_req.cancel()

               
        else:
            logging.info(f"{patient.id} cancelled, cc beds at {cc_beds.count}, hours remaining: {end_time - env.now}, surgical duration: {patient.surgery_duration}")
            schedule.schedule_patients([patient], env.now)

            patient.cancellations.append(env.now)

            cc_bed_req.cancel()


def ward(env, beds, bed_req, patient, metrics):
    yield env.timeout(patient.recovery_time)
    beds.release(bed_req)

    metrics["bed_event"].append((env.now, -1))
    metrics["beds"].append((env.now, beds.count))

    patient.discharge_time = env.now
    logging.info(f"{patient.id} discharged, beds now at: {beds.count}, {beds.users}.")
    

################ Driving the sim
def scheduler(env, beds, cc_beds, experiment, schedule, metrics):
    while True:
        if len(booked_appts:= schedule[env.now]) != 0:
            for _, scheduled_theatres in booked_appts.iterrows():
                if len(scheduled_theatres.patients) != 0:
                    logging.info(f"Sending {scheduled_theatres.patient_type}: {scheduled_theatres.patients} to surgery.")
                    env.process(surgery(env, beds, cc_beds, scheduled_theatres.patients, scheduled_theatres.hours_total, schedule, experiment, metrics))

        yield env.timeout(1)



################# Scheduling
class Schedule:
    def __init__(self, slots):
        self.__schedule = defaultdict(lambda : [])
        self.lcm = None

        self.__process_slots(slots)

        self.processed_schedule = self.__process_schedule(self.__schedule)

    def __process_slots(self, slots):
        repeat_periods = [slot.repeat_period for slot in slots]
        if repeat_periods != []:
            self.lcm = np.lcm.reduce(repeat_periods)
        else: 
            self.lcm = 1

        for slot in slots:
            repeat_slots = itertools.repeat(slot.start_time, self.lcm//slot.repeat_period)
            for multiplicative, start_time in enumerate(repeat_slots):

                processed_time = multiplicative * slot.repeat_period + start_time

                self.__schedule[processed_time] += [{slot.patient_type:slot.end_time - slot.start_time}]

        self.__schedule = dict(sorted(self.__schedule.items()))

    def __getitem__(self, time):
        while time > max(self.__schedule):
            multiple = max(self.__schedule) // self.lcm + 1
            later_schedule = {k+(self.lcm*multiple): v for k,v in self.__schedule.items()}
            self.__schedule |= later_schedule

            df = self.__process_schedule(later_schedule)
            self.processed_schedule = pd.concat([self.processed_schedule, df]).reset_index(drop=True)
            
        return self.processed_schedule.loc[(self.processed_schedule["hour"] == time), :]
    
    def __process_schedule(self, schedule):
        df = pd.DataFrame(
            [
                [time, list(slot.keys())[0], list(slot.values())[0], list(slot.values())[0], []
            ] for time, slots in schedule.items() for slot in slots],
            columns = ["hour", "patient_type", "hours_total", "hours_remaining", "patients"]
        )
        return df

    def schedule_patients(self, patients, time):
        patient_types = self.processed_schedule["patient_type"].unique()
        for patient in patients:
            patient_type = patient_types[np.where([patient_type in patient.id for patient_type in patient_types])[0]][0]

            sub_df = self.__find_slot(patient_type, patient, time)

            # if nothing matches in the schedule currently
            if len(sub_df) == 0:
                max_val = max(self.__schedule)
                self[max_val + 1]

                sub_df = self.__find_slot(patient_type, patient, time)

                if len(sub_df) == 0:
                    raise ValueError(f"Surgery duration for patient {patient.id} is too long for any surgery slot with a duration of {patient.surgery_duration}.")

            row_mask = self.processed_schedule.index == sub_df.index[0]
            self.processed_schedule.loc[row_mask, "hours_remaining"] -= patient.surgery_duration
            self.processed_schedule.loc[row_mask, "patients"].values[0].append(patient)

    def __find_slot(self, patient_type, patient, time):
        return self.processed_schedule.loc[
            (self.processed_schedule["patient_type"] == patient_type) 
            & (self.processed_schedule["hours_remaining"] > patient.surgery_duration)
            & (time < self.processed_schedule["hour"])
        ]
    
    def find_patient(self, patient):
        return self.processed_schedule.loc[self.processed_schedule["patients"].apply(lambda r: patient in r), :]

    def cancel_patient(self, patient):
        p_df = self.find_patient(patient).patients.to_list()[0]

        idx = p_df.index(patient)
        p_df.pop(idx)


slot = namedtuple("surgery_slot", ["start_time", "end_time", "patient_type", "repeat_period"])



##### Daily planning
def daily_planning(env, beds, schedule, experiment):
    while True:
        logging.info("New day!!!")
        logging.info(f"{beds.count} beds used, {beds.users}")

        emergency_patients = [
            patient for patient in experiment.patients 
            if "Emergency" in patient.id 
            and patient.surgical_time is None
            and (env.now - patient.arrival_time + 24) > experiment.max_emergency_wait
        ]
        
        emergencies_scheduled = schedule.processed_schedule.loc[
            (schedule.processed_schedule["hour"] >= env.now)
            & (schedule.processed_schedule["hour"] < env.now+24)
            & (schedule.processed_schedule["patient_type"] == "Emergency"), 
            "patients"
        ].values

        emergencies_scheduled = [*itertools.chain.from_iterable(emergencies_scheduled)]

        emergency_patients_breaching = [patient for patient in emergency_patients if patient not in emergencies_scheduled]
        logging.info(f"{len(emergency_patients_breaching)} patients are breaching! {emergency_patients_breaching}")

        non_emergencies_scheduled = schedule.processed_schedule.loc[
            (schedule.processed_schedule["hour"] >= env.now)
            & (schedule.processed_schedule["hour"] < env.now+24)
            & (schedule.processed_schedule["patient_type"] != "Emergency"), 
            ["patients", "hours_total", "hours_remaining"]
        ]

        for patient in emergency_patients_breaching:
            if len(non_emergencies_scheduled[non_emergencies_scheduled["hours_remaining"] > patient.surgery_duration]) != 0:
                logging.info(f"Slotting patient {patient} into a non-elective slot.")
                schedule.cancel_patient(patient)
                non_emergencies_scheduled.loc[non_emergencies_scheduled["hours_remaining"] > patient.surgery_duration, "patients"].iloc[0].insert(0, patient)
                
            elif len(non_emergencies_scheduled.loc[non_emergencies_scheduled["hours_total"] > patient.surgery_duration]):
                for _, non_em in non_emergencies_scheduled.loc[non_emergencies_scheduled["hours_total"] > patient.surgery_duration, :].iterrows():
                    ne_surg_duration = sum([patient.surgery_duration for patient in non_em.patients if "Emergency" not in patient.id])
                    if ne_surg_duration > patient.surgery_duration:
                        logging.info(f"Cancelling elective patients to fit patient {patient} in today.")
                        schedule.cancel_patient(patient)
                        non_em.patients.insert(0, patient)
                        non_em.hours_remaining -= patient.surgery_duration
                        while non_em.hours_remaining < 0:
                            non_em_patient = non_em.patients.pop()
                            schedule.schedule_patients([non_em_patient], non_em_patient.surgery_duration)
                            non_em.hours_remaining += non_em_patient.surgery_duration
                    break
                else:
                    logging.info(f"Patient {patient} is unable to be rescheduled today!")
            else:
                raise NotImplementedError(f"Unable to reschedule patient {patient}")
                # surgery too long for anything we have scheduled!!
        
        yield env.timeout(24)


################## Patient generators
# TODO: This could be a template?

def emergency_generator(env, experiment, schedule, prefix="Emergency"):
    for patient_count in itertools.count(start=1):
        inter_arrival_time = experiment.emergency_arrival_dist.sample()
        
        yield env.timeout(inter_arrival_time)
        
        p = Patient(f"{prefix}{patient_count}", arrival_time=env.now, surgery_duration=experiment.emergency_surgical_duration_dist.sample(), recovery_time=experiment.emergency_recovery_time_dist.sample())
        experiment.patients.append(p)

        logging.info(f"{env.now:.2f}: {p.id} referral arrives.")

        schedule.schedule_patients([p], env.now)

def elective_generator(env, experiment, schedule, prefix="Elective"):
    for patient_count in itertools.count(start=1):
        inter_arrival_time = experiment.emergency_arrival_dist.sample() # TODO: change this for elective
        
        yield env.timeout(inter_arrival_time)
        
        # TODO: change this for elective
        p = Patient(f"{prefix}{patient_count}", arrival_time=env.now, surgery_duration=experiment.elective_surgical_duration_dist.sample(), recovery_time=experiment.elective_recovery_time_dist.sample())
        experiment.patients.append(p)

        logging.info(f"{env.now:.2f}: {p.id} referral arrives.")

        schedule.schedule_patients([p], env.now)

def initial_elective_generator(env, experiment, schedule, prefix="Elective"):
    for patient_count in range(-experiment.initial_number_of_elective, 0):
        p = Patient(f"{prefix}{patient_count}", arrival_time=env.now, surgery_duration=experiment.elective_surgical_duration_dist.sample(), recovery_time=experiment.elective_recovery_time_dist.sample())
        experiment.patients.append(p)

        logging.info(f"{env.now:.2f}: {p.id} referral arrives.")

        schedule.schedule_patients([p], env.now)

def initial_emergency_generator(env, experiment, schedule, prefix="Emergency"):
    for patient_count in range(-experiment.initial_number_of_emergency, 0):
        p = Patient(f"{prefix}{patient_count}", arrival_time=env.now, surgery_duration=experiment.emergency_surgical_duration_dist.sample(), recovery_time=experiment.emergency_recovery_time_dist.sample())
        experiment.patients.append(p)

        logging.info(f"{env.now:.2f}: {p.id} referral arrives.")

        schedule.schedule_patients([p], env.now)


# TODO: As patients are generated, add to schedule?
        # What about emergency?
        # What happens if a scheduled surgery doesn't have a bed available??


# ########### Run sim
# def single_run(experiment):
#     schedule = Schedule(
#         [
#             slot(8, 18, "Emergency", DAILY), # 8 - 6 every day 7 days a week
#             slot(8, 14, "Elective", WEEKLY), # Once a week between 8 and 2
#             slot(32, 42, "Elective", WEEKLY), # Once a week between 8am and 6pm on Tuesday
#             slot(34, 36, "Elective", WEEKLY),
#             slot(8, 12, "Elective", DAILY),
#             slot(10, 15, "Emergency", DAILY),
#             slot(56, 60, "Emergency", WEEKLY),
#             slot(24, 48, "Elective", WEEKLY), # Catch all, this will be tidied up when real data is put in
#             slot(24, 48, "Emergency", WEEKLY), # Catch all, this will be tidied up when real data is put in
#         ]
#     )

#     metrics = defaultdict(lambda : [])

#     env = simpy.Environment()
#     logger = setup_logger(env, logging.INFO)

#     beds = simpy.Resource(env, capacity=NUM_BEDS)
#     cc_beds = simpy.Resource(env, capacity=NUM_CC_BEDS)

#     initial_elective_generator(env, experiment, schedule)
#     initial_emergency_generator(env, experiment, schedule)

#     env.process(emergency_generator(env, experiment, schedule))
#     env.process(daily_planning(env, beds, schedule, experiment))
#     env.process(scheduler(env, beds, cc_beds, experiment, schedule, metrics))

#     env.run(until=RUN_LENGTH)