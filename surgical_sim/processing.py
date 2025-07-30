import itertools
import logging

from .resources import surgery


def scheduler(env, beds, cc_beds, experiment, schedule, metrics):
    """
    Continuously checks the current simulation time for scheduled surgeries and dispatches them.

    Args:
        env (simpy.Environment): The simulation environment.
        beds (simpy.Resource): Resource representing general hospital beds.
        cc_beds (simpy.Resource): Resource representing critical care beds.
        experiment (Any): Object containing experiment configuration and patient data.
        schedule (Any): Schedule object with time-indexed patient assignments.
        metrics (Dict[str, list]): Dictionary for tracking simulation metrics.

    Yields:
        simpy.events.Event: A SimPy timeout event that triggers every simulation hour.
    """
    while True:
        if len(booked_appts := schedule[env.now]) != 0:
            for _, scheduled_theatres in booked_appts.iterrows():
                if len(scheduled_theatres.patients) != 0:
                    logging.info(
                        f"Sending {scheduled_theatres.patient_type}: {scheduled_theatres.patients} to surgery."
                    )
                    env.process(
                        surgery(
                            env,
                            beds,
                            cc_beds,
                            scheduled_theatres.patients,
                            scheduled_theatres.hours_total,
                            schedule,
                            experiment,
                            metrics,
                        )
                    )

        yield env.timeout(1)


def daily_planning(env, beds, schedule, experiment):
    """
    Performs daily planning to ensure emergency patients are scheduled within acceptable wait times.

    Args:
        env (simpy.Environment): The simulation environment.
        beds (simpy.Resource): Resource representing general hospital beds.
        schedule (Any): Schedule object with time-indexed patient assignments.
        experiment (Any): Object containing experiment configuration and patient data.

    Yields:
        simpy.events.Event: A SimPy timeout event that triggers every 24 simulation hours.
    """
    while True:
        logging.info("New day!!!")
        logging.info(f"{beds.count} beds used, {beds.users}")

        emergency_patients = [
            patient
            for patient in experiment.patients
            if "Emergency" in patient.id
            and patient.surgical_time is None
            and (env.now - patient.arrival_time + 24) > experiment.max_emergency_wait
        ]

        emergencies_scheduled = schedule.processed_schedule.loc[
            (schedule.processed_schedule["hour"] >= env.now)
            & (schedule.processed_schedule["hour"] < env.now + 24)
            & (schedule.processed_schedule["patient_type"] == "Emergency"),
            "patients",
        ].values

        emergencies_scheduled = [*itertools.chain.from_iterable(emergencies_scheduled)]

        emergency_patients_breaching = [
            patient
            for patient in emergency_patients
            if patient not in emergencies_scheduled
        ]
        logging.info(
            f"{len(emergency_patients_breaching)} patients are breaching! {emergency_patients_breaching}"
        )

        non_emergencies_scheduled = schedule.processed_schedule.loc[
            (schedule.processed_schedule["hour"] >= env.now)
            & (schedule.processed_schedule["hour"] < env.now + 24)
            & (schedule.processed_schedule["patient_type"] != "Emergency"),
            ["patients", "hours_total", "hours_remaining"],
        ]

        for patient in emergency_patients_breaching:
            if (
                len(
                    non_emergencies_scheduled[
                        non_emergencies_scheduled["hours_remaining"]
                        > patient.surgery_duration
                    ]
                )
                != 0
            ):
                logging.info(f"Slotting patient {patient} into a non-elective slot.")
                schedule.cancel_patient(patient)
                non_emergencies_scheduled.loc[
                    non_emergencies_scheduled["hours_remaining"]
                    > patient.surgery_duration,
                    "patients",
                ].iloc[0].insert(0, patient)

            elif len(
                non_emergencies_scheduled.loc[
                    non_emergencies_scheduled["hours_total"] > patient.surgery_duration
                ]
            ):
                for _, non_em in non_emergencies_scheduled.loc[
                    non_emergencies_scheduled["hours_total"] > patient.surgery_duration,
                    :,
                ].iterrows():
                    ne_surg_duration = sum(
                        [
                            patient.surgery_duration
                            for patient in non_em.patients
                            if "Emergency" not in patient.id
                        ]
                    )
                    if ne_surg_duration > patient.surgery_duration:
                        logging.info(
                            f"Cancelling elective patients to fit patient {patient} in today."
                        )
                        schedule.cancel_patient(patient)
                        non_em.patients.insert(0, patient)
                        non_em.hours_remaining -= patient.surgery_duration
                        while non_em.hours_remaining < 0:
                            non_em_patient = non_em.patients.pop()
                            schedule.schedule_patients(
                                [non_em_patient], non_em_patient.surgery_duration
                            )
                            non_em.hours_remaining += non_em_patient.surgery_duration
                    break
                else:
                    logging.info(
                        f"Patient {patient} is unable to be rescheduled today!"
                    )
            else:
                logging.info(
                    f"Patient {patient} is unable to be rescheduled today - no available slots!"
                )

                # surgery too long for anything we have scheduled!!

        yield env.timeout(24)
