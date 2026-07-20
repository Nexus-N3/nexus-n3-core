from nexus_n3.robots.models.commands import MotionAction, MotionCommand


def test_motion_command_keeps_action_and_speed():
    cmd = MotionCommand(action=MotionAction.FWD, speed=0.5)

    assert cmd.action == MotionAction.FWD
    assert cmd.speed == 0.5


def test_motion_command_stop_factory():
    cmd = MotionCommand.stop(robot_id="nexus-rover-01", source="test")

    assert cmd.action == MotionAction.STOP
    assert cmd.speed == 0.0
    assert cmd.robot_id == "nexus-rover-01"
    assert cmd.source == "test"


def test_normalized_speed_clamps_high_value():
    cmd = MotionCommand(action=MotionAction.FWD, speed=2.0)

    assert cmd.normalized_speed() == 1.0


def test_normalized_speed_clamps_negative_value():
    cmd = MotionCommand(action=MotionAction.FWD, speed=-0.5)

    assert cmd.normalized_speed() == 0.0


def test_normalized_speed_allows_valid_value():
    cmd = MotionCommand(action=MotionAction.FWD, speed=0.75)

    assert cmd.normalized_speed() == 0.75