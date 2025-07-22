import itertools
import logging
from dataclasses import dataclass, field
from typing import List


@dataclass
class Patient:
    """
    Represents a patient in the simulation with relevant surgical and recovery attributes.

    Attributes:
        id (str): Unique identifier for the patient.
        arrival_time (Optional[int]): Time the patient arrives in the simulation.
        surgical_time (Optional[int]): Time the patient undergoes surgery.
        discharge_time (Optional[int]): Time the patient is discharged.
        surgery_duration (Optional[int]): Duration of the surgery.
        recovery_time (Optional[int]): Duration of the recovery period.
        cancellations (List[int]): List of times the patient was cancelled.
    """

    id: str
    arrival_time: int = None
    surgical_time: int = None
    discharge_time: int = None
    surgery_duration: int = None
    recovery_time: int = None
    cancellations: List = field(default_factory=lambda: [])


def emergency_generator(env, experiment, schedule, prefix="Emergency"):
    """
    Continuously generates emergency patients at intervals defined by the experiment.

    Args:
        env (simpy.Environment): The simulation environment.
        experiment (Any): Object containing emergency distribution samplers and patient list.
        schedule (Any): Schedule object used to assign patients to slots.
        prefix (str, optional): Prefix for patient IDs. Defaults to "Emergency".

    Yields:
        Generator: SimPy timeout events between patient arrivals.
    """
    for patient_count in itertools.count(start=1):
        inter_arrival_time = experiment.emergency_arrival_dist.sample()

        yield env.timeout(inter_arrival_time)

        p = Patient(
            f"{prefix}{patient_count}",
            arrival_time=env.now,
            surgery_duration=experiment.emergency_surgical_duration_dist.sample(),
            recovery_time=experiment.emergency_recovery_time_dist.sample(),
        )
        experiment.patients.append(p)

        logging.info(f"{env.now:.2f}: {p.id} referral arrives.")

        schedule.schedule_patients([p], env.now)


def elective_generator(env, experiment, schedule, prefix="Elective"):
    """
    Continuously generates elective patients at intervals defined by the experiment.

    Args:
        env (simpy.Environment): The simulation environment.
        experiment (Any): Object containing elective distribution samplers and patient list.
        schedule (Any): Schedule object used to assign patients to slots.
        prefix (str, optional): Prefix for patient IDs. Defaults to "Elective".

    Yields:
        Generator: SimPy timeout events between patient arrivals.
    """
    for patient_count in itertools.count(start=1):
        inter_arrival_time = experiment.elective_arrival_dist.sample()

        yield env.timeout(inter_arrival_time)

        p = Patient(
            f"{prefix}{patient_count}",
            arrival_time=env.now,
            surgery_duration=experiment.elective_surgical_duration_dist.sample(),
            recovery_time=experiment.elective_recovery_time_dist.sample(),
        )
        experiment.patients.append(p)

        logging.info(f"{env.now:.2f}: {p.id} referral arrives.")

        schedule.schedule_patients([p], env.now)


def initial_elective_generator(env, experiment, schedule, prefix="Elective"):
    """
    Generates an initial batch of elective patients at simulation start.

    Args:
        env (simpy.Environment): The simulation environment.
        experiment (Any): Object containing elective distribution samplers and patient list.
        schedule (Any): Schedule object used to assign patients to slots.
        prefix (str, optional): Prefix for patient IDs. Defaults to "Elective".
    """
    for patient_count in range(-experiment.initial_number_of_elective, 0):
        p = Patient(
            f"{prefix}{patient_count}",
            arrival_time=env.now,
            surgery_duration=experiment.elective_surgical_duration_dist.sample(),
            recovery_time=experiment.elective_recovery_time_dist.sample(),
        )
        experiment.patients.append(p)

        logging.info(f"{env.now:.2f}: {p.id} referral arrives.")

        schedule.schedule_patients([p], env.now)


def initial_emergency_generator(env, experiment, schedule, prefix="Emergency"):
    """
    Generates an initial batch of emergency patients at simulation start.

    Args:
        env (simpy.Environment): The simulation environment.
        experiment (Any): Object containing emergency distribution samplers and patient list.
        schedule (Any): Schedule object used to assign patients to slots.
        prefix (str, optional): Prefix for patient IDs. Defaults to "Emergency".
    """
    for patient_count in range(-experiment.initial_number_of_emergency, 0):
        p = Patient(
            f"{prefix}{patient_count}",
            arrival_time=env.now,
            surgery_duration=experiment.emergency_surgical_duration_dist.sample(),
            recovery_time=experiment.emergency_recovery_time_dist.sample(),
        )
        experiment.patients.append(p)

        logging.info(f"{env.now:.2f}: {p.id} referral arrives.")

        schedule.schedule_patients([p], env.now)
