import logging

__all__ = ["surgery", "ward"]


def surgery(
    env, beds, cc_beds, patients, hours_available, schedule, experiment, metrics
):
    """
    Simulates the surgical process for a list of patients within a given time window.

    Args:
        env (simpy.Environment): The simulation environment.
        beds (simpy.Resource): Resource representing general hospital beds.
        cc_beds (simpy.Resource): Resource representing critical care beds.
        patients (List[Any]): List of patient objects, each with attributes like `surgery_duration`, `id`, and `cancellations`.
        hours_available (float): Number of hours available for surgeries in the current simulation window.
        schedule (Any): Scheduling object with a method `schedule_patients` to reschedule patients.
        experiment (Any): Placeholder for experiment configuration or metadata (not used directly in this function).
        metrics (Dict[str, List[Any]]): Dictionary to record various simulation metrics such as bed usage and surgical events.

    Returns:
        simpy.events.Event: A SimPy event that represents the completion of the surgery process.
    """
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
            timeout = env.timeout(
                max(0, end_time - env.now - patient.surgery_duration - 1)
            )
            result = yield env.any_of([bed_req, timeout])

            if bed_req in result:
                metrics["beds"].append((env.now, beds.count))

                logging.info(f"Assigned bed to {patient}")
                metrics["bed_event"].append((env.now, 1))

                surgical_time = env.now
                metrics["surgical_event"].append((surgical_time, 1))

                patient.surgical_time = surgical_time

                yield env.timeout(patient.surgery_duration)
                logging.info(
                    f"{patient.id} had surgery for: {patient.surgery_duration} hours"
                )
                metrics["surgical_event"].append((env.now, -1))

                # TODO: random select if patient goes to CC (what to do with bed until then?)
                cc_beds.release(cc_bed_req)
                metrics["cc_bed_event"].append((env.now, -1))

                env.process(ward(env, beds, bed_req, patient, metrics))

                metrics["beds"].append((env.now, beds.count))

            else:
                logging.info(
                    f"{patient.id} cancelled, beds at {beds.count}, hours remaining: {end_time - env.now}, surgical duration: {patient.surgery_duration}"
                )
                schedule.schedule_patients([patient], env.now)

                cc_beds.release(cc_bed_req)
                metrics["cc_bed_event"].append((env.now, -1))

                patient.cancellations.append(env.now)

                bed_req.cancel()

        else:
            logging.info(
                f"{patient.id} cancelled, cc beds at {cc_beds.count}, hours remaining: {end_time - env.now}, surgical duration: {patient.surgery_duration}"
            )
            schedule.schedule_patients([patient], env.now)

            patient.cancellations.append(env.now)

            cc_bed_req.cancel()


def ward(env, beds, bed_req, patient, metrics):
    """
    Simulates the post-surgery recovery process for a patient in a hospital ward.

    Args:
        env (simpy.Environment): The simulation environment.
        beds (simpy.Resource): Resource representing general hospital beds.
        bed_req (simpy.Resource.request): The specific bed request allocated to the patient.
        patient (Any): The patient object, expected to have attributes like `recovery_time`, `id`, and `discharge_time`.
        metrics (Dict[str, list]): Dictionary for tracking simulation metrics such as bed usage and discharge events.

    Returns:
        simpy.events.Event: A SimPy event representing the completion of the recovery process.
    """
    yield env.timeout(patient.recovery_time)
    beds.release(bed_req)

    metrics["bed_event"].append((env.now, -1))
    metrics["beds"].append((env.now, beds.count))

    patient.discharge_time = env.now
    logging.info(f"{patient.id} discharged, beds now at: {beds.count}, {beds.users}.")
