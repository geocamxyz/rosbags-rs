#!/usr/bin/env python3
"""
Generate test ROS2 bag files for testing a Rust rosbag project.

This script creates two separate bag files (SQLite3 and MCAP format) with
randomly selected message types from the provided list, targeting ROS2 Foxy
distribution compatibility.

Requirements:
- rosbags package: pip install rosbags
- numpy: pip install numpy

Usage:
    python generate_test_bags.py

Output:
- test_bag_sqlite3.db3 (SQLite3 format)
- test_bag_mcap.mcap (MCAP format)
"""

import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np

try:
    from rosbags.rosbag2 import Writer
    from rosbags.rosbag2.enums import StoragePlugin
    from rosbags.typesys import Stores, get_typestore
    from rosbags.typesys.stores.ros2_foxy import (  # geometry_msgs; nav_msgs; sensor_msgs; std_msgs; stereo_msgs; tf2_msgs; builtin_interfaces for timestamps
        builtin_interfaces__msg__Duration,
        builtin_interfaces__msg__Time,
        geometry_msgs__msg__Accel,
        geometry_msgs__msg__AccelStamped,
        geometry_msgs__msg__AccelWithCovariance,
        geometry_msgs__msg__AccelWithCovarianceStamped,
        geometry_msgs__msg__Inertia,
        geometry_msgs__msg__InertiaStamped,
        geometry_msgs__msg__Point,
        geometry_msgs__msg__Point32,
        geometry_msgs__msg__PointStamped,
        geometry_msgs__msg__Polygon,
        geometry_msgs__msg__PolygonStamped,
        geometry_msgs__msg__Pose,
        geometry_msgs__msg__Pose2D,
        geometry_msgs__msg__PoseArray,
        geometry_msgs__msg__PoseStamped,
        geometry_msgs__msg__PoseWithCovariance,
        geometry_msgs__msg__PoseWithCovarianceStamped,
        geometry_msgs__msg__Quaternion,
        geometry_msgs__msg__QuaternionStamped,
        geometry_msgs__msg__Transform,
        geometry_msgs__msg__TransformStamped,
        geometry_msgs__msg__Twist,
        geometry_msgs__msg__TwistStamped,
        geometry_msgs__msg__TwistWithCovariance,
        geometry_msgs__msg__TwistWithCovarianceStamped,
        geometry_msgs__msg__Vector3,
        geometry_msgs__msg__Vector3Stamped,
        geometry_msgs__msg__Wrench,
        geometry_msgs__msg__WrenchStamped,
        nav_msgs__msg__GridCells,
        nav_msgs__msg__MapMetaData,
        nav_msgs__msg__OccupancyGrid,
        nav_msgs__msg__Odometry,
        nav_msgs__msg__Path,
        sensor_msgs__msg__BatteryState,
        sensor_msgs__msg__CameraInfo,
        sensor_msgs__msg__ChannelFloat32,
        sensor_msgs__msg__CompressedImage,
        sensor_msgs__msg__FluidPressure,
        sensor_msgs__msg__Illuminance,
        sensor_msgs__msg__Image,
        sensor_msgs__msg__Imu,
        sensor_msgs__msg__JointState,
        sensor_msgs__msg__Joy,
        sensor_msgs__msg__JoyFeedback,
        sensor_msgs__msg__JoyFeedbackArray,
        sensor_msgs__msg__LaserEcho,
        sensor_msgs__msg__LaserScan,
        sensor_msgs__msg__MagneticField,
        sensor_msgs__msg__MultiDOFJointState,
        sensor_msgs__msg__MultiEchoLaserScan,
        sensor_msgs__msg__NavSatFix,
        sensor_msgs__msg__NavSatStatus,
        sensor_msgs__msg__PointCloud,
        sensor_msgs__msg__PointCloud2,
        sensor_msgs__msg__PointField,
        sensor_msgs__msg__Range,
        sensor_msgs__msg__RegionOfInterest,
        sensor_msgs__msg__RelativeHumidity,
        sensor_msgs__msg__Temperature,
        sensor_msgs__msg__TimeReference,
        std_msgs__msg__Bool,
        std_msgs__msg__Byte,
        std_msgs__msg__ByteMultiArray,
        std_msgs__msg__Char,
        std_msgs__msg__ColorRGBA,
        std_msgs__msg__Empty,
        std_msgs__msg__Float32,
        std_msgs__msg__Float32MultiArray,
        std_msgs__msg__Float64,
        std_msgs__msg__Float64MultiArray,
        std_msgs__msg__Header,
        std_msgs__msg__Int8,
        std_msgs__msg__Int8MultiArray,
        std_msgs__msg__Int16,
        std_msgs__msg__Int16MultiArray,
        std_msgs__msg__Int32,
        std_msgs__msg__Int32MultiArray,
        std_msgs__msg__Int64,
        std_msgs__msg__Int64MultiArray,
        std_msgs__msg__MultiArrayDimension,
        std_msgs__msg__MultiArrayLayout,
        std_msgs__msg__String,
        std_msgs__msg__UInt8,
        std_msgs__msg__UInt8MultiArray,
        std_msgs__msg__UInt16,
        std_msgs__msg__UInt16MultiArray,
        std_msgs__msg__UInt32,
        std_msgs__msg__UInt32MultiArray,
        std_msgs__msg__UInt64,
        std_msgs__msg__UInt64MultiArray,
        stereo_msgs__msg__DisparityImage,
        tf2_msgs__msg__TF2Error,
        tf2_msgs__msg__TFMessage,
    )
except ImportError as e:
    print(f'Error importing rosbags: {e}')
    print('Please install rosbags: pip install rosbags')
    exit(1)


# Define all available message types with their classes and topic prefixes
MESSAGE_TYPES = {
    # geometry_msgs
    'geometry_msgs/msg/Accel': (geometry_msgs__msg__Accel, '/test/geometry_msgs/accel'),
    'geometry_msgs/msg/AccelStamped': (geometry_msgs__msg__AccelStamped, '/test/geometry_msgs/accel_stamped'),
    'geometry_msgs/msg/AccelWithCovariance': (
        geometry_msgs__msg__AccelWithCovariance,
        '/test/geometry_msgs/accel_with_covariance',
    ),
    'geometry_msgs/msg/AccelWithCovarianceStamped': (
        geometry_msgs__msg__AccelWithCovarianceStamped,
        '/test/geometry_msgs/accel_with_covariance_stamped',
    ),
    'geometry_msgs/msg/Inertia': (geometry_msgs__msg__Inertia, '/test/geometry_msgs/inertia'),
    'geometry_msgs/msg/InertiaStamped': (geometry_msgs__msg__InertiaStamped, '/test/geometry_msgs/inertia_stamped'),
    'geometry_msgs/msg/Point': (geometry_msgs__msg__Point, '/test/geometry_msgs/point'),
    'geometry_msgs/msg/Point32': (geometry_msgs__msg__Point32, '/test/geometry_msgs/point32'),
    'geometry_msgs/msg/PointStamped': (geometry_msgs__msg__PointStamped, '/test/geometry_msgs/point_stamped'),
    'geometry_msgs/msg/Polygon': (geometry_msgs__msg__Polygon, '/test/geometry_msgs/polygon'),
    'geometry_msgs/msg/PolygonStamped': (geometry_msgs__msg__PolygonStamped, '/test/geometry_msgs/polygon_stamped'),
    'geometry_msgs/msg/Pose': (geometry_msgs__msg__Pose, '/test/geometry_msgs/pose'),
    'geometry_msgs/msg/Pose2D': (geometry_msgs__msg__Pose2D, '/test/geometry_msgs/pose2d'),
    'geometry_msgs/msg/PoseArray': (geometry_msgs__msg__PoseArray, '/test/geometry_msgs/pose_array'),
    'geometry_msgs/msg/PoseStamped': (geometry_msgs__msg__PoseStamped, '/test/geometry_msgs/pose_stamped'),
    'geometry_msgs/msg/PoseWithCovariance': (
        geometry_msgs__msg__PoseWithCovariance,
        '/test/geometry_msgs/pose_with_covariance',
    ),
    'geometry_msgs/msg/PoseWithCovarianceStamped': (
        geometry_msgs__msg__PoseWithCovarianceStamped,
        '/test/geometry_msgs/pose_with_covariance_stamped',
    ),
    'geometry_msgs/msg/Quaternion': (geometry_msgs__msg__Quaternion, '/test/geometry_msgs/quaternion'),
    'geometry_msgs/msg/QuaternionStamped': (
        geometry_msgs__msg__QuaternionStamped,
        '/test/geometry_msgs/quaternion_stamped',
    ),
    'geometry_msgs/msg/Transform': (geometry_msgs__msg__Transform, '/test/geometry_msgs/transform'),
    'geometry_msgs/msg/TransformStamped': (
        geometry_msgs__msg__TransformStamped,
        '/test/geometry_msgs/transform_stamped',
    ),
    'geometry_msgs/msg/Twist': (geometry_msgs__msg__Twist, '/test/geometry_msgs/twist'),
    'geometry_msgs/msg/TwistStamped': (geometry_msgs__msg__TwistStamped, '/test/geometry_msgs/twist_stamped'),
    'geometry_msgs/msg/TwistWithCovariance': (
        geometry_msgs__msg__TwistWithCovariance,
        '/test/geometry_msgs/twist_with_covariance',
    ),
    'geometry_msgs/msg/TwistWithCovarianceStamped': (
        geometry_msgs__msg__TwistWithCovarianceStamped,
        '/test/geometry_msgs/twist_with_covariance_stamped',
    ),
    'geometry_msgs/msg/Vector3': (geometry_msgs__msg__Vector3, '/test/geometry_msgs/vector3'),
    'geometry_msgs/msg/Vector3Stamped': (geometry_msgs__msg__Vector3Stamped, '/test/geometry_msgs/vector3_stamped'),
    'geometry_msgs/msg/Wrench': (geometry_msgs__msg__Wrench, '/test/geometry_msgs/wrench'),
    'geometry_msgs/msg/WrenchStamped': (geometry_msgs__msg__WrenchStamped, '/test/geometry_msgs/wrench_stamped'),
    # nav_msgs
    'nav_msgs/msg/GridCells': (nav_msgs__msg__GridCells, '/test/nav_msgs/grid_cells'),
    'nav_msgs/msg/MapMetaData': (nav_msgs__msg__MapMetaData, '/test/nav_msgs/map_metadata'),
    'nav_msgs/msg/OccupancyGrid': (nav_msgs__msg__OccupancyGrid, '/test/nav_msgs/occupancy_grid'),
    'nav_msgs/msg/Odometry': (nav_msgs__msg__Odometry, '/test/nav_msgs/odometry'),
    'nav_msgs/msg/Path': (nav_msgs__msg__Path, '/test/nav_msgs/path'),
    # sensor_msgs
    'sensor_msgs/msg/BatteryState': (sensor_msgs__msg__BatteryState, '/test/sensor_msgs/battery_state'),
    'sensor_msgs/msg/CameraInfo': (sensor_msgs__msg__CameraInfo, '/test/sensor_msgs/camera_info'),
    'sensor_msgs/msg/ChannelFloat32': (sensor_msgs__msg__ChannelFloat32, '/test/sensor_msgs/channel_float32'),
    'sensor_msgs/msg/CompressedImage': (sensor_msgs__msg__CompressedImage, '/test/sensor_msgs/compressed_image'),
    'sensor_msgs/msg/FluidPressure': (sensor_msgs__msg__FluidPressure, '/test/sensor_msgs/fluid_pressure'),
    'sensor_msgs/msg/Illuminance': (sensor_msgs__msg__Illuminance, '/test/sensor_msgs/illuminance'),
    'sensor_msgs/msg/Image': (sensor_msgs__msg__Image, '/test/sensor_msgs/image'),
    'sensor_msgs/msg/Imu': (sensor_msgs__msg__Imu, '/test/sensor_msgs/imu'),
    'sensor_msgs/msg/JointState': (sensor_msgs__msg__JointState, '/test/sensor_msgs/joint_state'),
    'sensor_msgs/msg/Joy': (sensor_msgs__msg__Joy, '/test/sensor_msgs/joy'),
    'sensor_msgs/msg/JoyFeedback': (sensor_msgs__msg__JoyFeedback, '/test/sensor_msgs/joy_feedback'),
    'sensor_msgs/msg/JoyFeedbackArray': (sensor_msgs__msg__JoyFeedbackArray, '/test/sensor_msgs/joy_feedback_array'),
    'sensor_msgs/msg/LaserEcho': (sensor_msgs__msg__LaserEcho, '/test/sensor_msgs/laser_echo'),
    'sensor_msgs/msg/LaserScan': (sensor_msgs__msg__LaserScan, '/test/sensor_msgs/laser_scan'),
    'sensor_msgs/msg/MagneticField': (sensor_msgs__msg__MagneticField, '/test/sensor_msgs/magnetic_field'),
    'sensor_msgs/msg/MultiDOFJointState': (
        sensor_msgs__msg__MultiDOFJointState,
        '/test/sensor_msgs/multi_dof_joint_state',
    ),
    'sensor_msgs/msg/MultiEchoLaserScan': (
        sensor_msgs__msg__MultiEchoLaserScan,
        '/test/sensor_msgs/multi_echo_laser_scan',
    ),
    'sensor_msgs/msg/NavSatFix': (sensor_msgs__msg__NavSatFix, '/test/sensor_msgs/nav_sat_fix'),
    'sensor_msgs/msg/NavSatStatus': (sensor_msgs__msg__NavSatStatus, '/test/sensor_msgs/nav_sat_status'),
    'sensor_msgs/msg/PointCloud': (sensor_msgs__msg__PointCloud, '/test/sensor_msgs/point_cloud'),
    'sensor_msgs/msg/PointCloud2': (sensor_msgs__msg__PointCloud2, '/test/sensor_msgs/point_cloud2'),
    'sensor_msgs/msg/PointField': (sensor_msgs__msg__PointField, '/test/sensor_msgs/point_field'),
    'sensor_msgs/msg/Range': (sensor_msgs__msg__Range, '/test/sensor_msgs/range'),
    'sensor_msgs/msg/RegionOfInterest': (sensor_msgs__msg__RegionOfInterest, '/test/sensor_msgs/region_of_interest'),
    'sensor_msgs/msg/RelativeHumidity': (sensor_msgs__msg__RelativeHumidity, '/test/sensor_msgs/relative_humidity'),
    'sensor_msgs/msg/Temperature': (sensor_msgs__msg__Temperature, '/test/sensor_msgs/temperature'),
    'sensor_msgs/msg/TimeReference': (sensor_msgs__msg__TimeReference, '/test/sensor_msgs/time_reference'),
    # std_msgs
    'std_msgs/msg/Bool': (std_msgs__msg__Bool, '/test/std_msgs/bool'),
    'std_msgs/msg/Byte': (std_msgs__msg__Byte, '/test/std_msgs/byte'),
    'std_msgs/msg/ByteMultiArray': (std_msgs__msg__ByteMultiArray, '/test/std_msgs/byte_multi_array'),
    'std_msgs/msg/Char': (std_msgs__msg__Char, '/test/std_msgs/char'),
    'std_msgs/msg/ColorRGBA': (std_msgs__msg__ColorRGBA, '/test/std_msgs/color_rgba'),
    'std_msgs/msg/Empty': (std_msgs__msg__Empty, '/test/std_msgs/empty'),
    'std_msgs/msg/Float32': (std_msgs__msg__Float32, '/test/std_msgs/float32'),
    'std_msgs/msg/Float32MultiArray': (std_msgs__msg__Float32MultiArray, '/test/std_msgs/float32_multi_array'),
    'std_msgs/msg/Float64': (std_msgs__msg__Float64, '/test/std_msgs/float64'),
    'std_msgs/msg/Float64MultiArray': (std_msgs__msg__Float64MultiArray, '/test/std_msgs/float64_multi_array'),
    'std_msgs/msg/Header': (std_msgs__msg__Header, '/test/std_msgs/header'),
    'std_msgs/msg/Int16': (std_msgs__msg__Int16, '/test/std_msgs/int16'),
    'std_msgs/msg/Int16MultiArray': (std_msgs__msg__Int16MultiArray, '/test/std_msgs/int16_multi_array'),
    'std_msgs/msg/Int32': (std_msgs__msg__Int32, '/test/std_msgs/int32'),
    'std_msgs/msg/Int32MultiArray': (std_msgs__msg__Int32MultiArray, '/test/std_msgs/int32_multi_array'),
    'std_msgs/msg/Int64': (std_msgs__msg__Int64, '/test/std_msgs/int64'),
    'std_msgs/msg/Int64MultiArray': (std_msgs__msg__Int64MultiArray, '/test/std_msgs/int64_multi_array'),
    'std_msgs/msg/Int8': (std_msgs__msg__Int8, '/test/std_msgs/int8'),
    'std_msgs/msg/Int8MultiArray': (std_msgs__msg__Int8MultiArray, '/test/std_msgs/int8_multi_array'),
    'std_msgs/msg/MultiArrayDimension': (std_msgs__msg__MultiArrayDimension, '/test/std_msgs/multi_array_dimension'),
    'std_msgs/msg/MultiArrayLayout': (std_msgs__msg__MultiArrayLayout, '/test/std_msgs/multi_array_layout'),
    'std_msgs/msg/String': (std_msgs__msg__String, '/test/std_msgs/string'),
    'std_msgs/msg/UInt16': (std_msgs__msg__UInt16, '/test/std_msgs/uint16'),
    'std_msgs/msg/UInt16MultiArray': (std_msgs__msg__UInt16MultiArray, '/test/std_msgs/uint16_multi_array'),
    'std_msgs/msg/UInt32': (std_msgs__msg__UInt32, '/test/std_msgs/uint32'),
    'std_msgs/msg/UInt32MultiArray': (std_msgs__msg__UInt32MultiArray, '/test/std_msgs/uint32_multi_array'),
    'std_msgs/msg/UInt64': (std_msgs__msg__UInt64, '/test/std_msgs/uint64'),
    'std_msgs/msg/UInt64MultiArray': (std_msgs__msg__UInt64MultiArray, '/test/std_msgs/uint64_multi_array'),
    'std_msgs/msg/UInt8': (std_msgs__msg__UInt8, '/test/std_msgs/uint8'),
    'std_msgs/msg/UInt8MultiArray': (std_msgs__msg__UInt8MultiArray, '/test/std_msgs/uint8_multi_array'),
    # stereo_msgs
    'stereo_msgs/msg/DisparityImage': (stereo_msgs__msg__DisparityImage, '/test/stereo_msgs/disparity_image'),
    # tf2_msgs
    'tf2_msgs/msg/TF2Error': (tf2_msgs__msg__TF2Error, '/test/tf2_msgs/tf2_error'),
    'tf2_msgs/msg/TFMessage': (tf2_msgs__msg__TFMessage, '/test/tf2_msgs/tf_message'),
}


def create_header(frame_id: str = 'test_frame') -> std_msgs__msg__Header:
    """Create a standard ROS header with current timestamp."""
    current_time_ns = int(time.time() * 1e9)
    return std_msgs__msg__Header(
        stamp=builtin_interfaces__msg__Time(
            sec=current_time_ns // 1_000_000_000, nanosec=current_time_ns % 1_000_000_000
        ),
        frame_id=frame_id,
    )


def create_sample_message(msg_class: Any, msg_type: str) -> Any:
    """Create a sample message with realistic data for the given message type."""

    # Simple message types
    if msg_type == 'std_msgs/msg/Bool':
        return msg_class(data=True)
    elif msg_type == 'std_msgs/msg/Byte':
        return msg_class(data=42)
    elif msg_type == 'std_msgs/msg/Char':
        return msg_class(data=65)  # 'A'
    elif msg_type == 'std_msgs/msg/Empty':
        return msg_class()
    elif msg_type == 'std_msgs/msg/Float32':
        return msg_class(data=3.14159)
    elif msg_type == 'std_msgs/msg/Float64':
        return msg_class(data=2.71828)
    elif msg_type == 'std_msgs/msg/Int8':
        return msg_class(data=-42)
    elif msg_type == 'std_msgs/msg/Int16':
        return msg_class(data=-1000)
    elif msg_type == 'std_msgs/msg/Int32':
        return msg_class(data=-100000)
    elif msg_type == 'std_msgs/msg/Int64':
        return msg_class(data=-10000000000)
    elif msg_type == 'std_msgs/msg/UInt8':
        return msg_class(data=255)
    elif msg_type == 'std_msgs/msg/UInt16':
        return msg_class(data=65535)
    elif msg_type == 'std_msgs/msg/UInt32':
        return msg_class(data=4294967295)
    elif msg_type == 'std_msgs/msg/UInt64':
        return msg_class(data=18446744073709551615)
    elif msg_type == 'std_msgs/msg/String':
        return msg_class(data='Hello, ROS2!')
    elif msg_type == 'std_msgs/msg/Header':
        return create_header()
    elif msg_type == 'std_msgs/msg/ColorRGBA':
        return msg_class(r=1.0, g=0.5, b=0.0, a=0.8)
    elif msg_type == 'std_msgs/msg/MultiArrayLayout':
        return msg_class(dim=[std_msgs__msg__MultiArrayDimension(label='x', size=3, stride=3)], data_offset=0)
    elif msg_type == 'std_msgs/msg/MultiArrayDimension':
        return msg_class(label='dimension', size=10, stride=10)

    # Geometry messages
    elif msg_type == 'geometry_msgs/msg/Point':
        return msg_class(x=1.0, y=2.0, z=3.0)
    elif msg_type == 'geometry_msgs/msg/Point32':
        return msg_class(x=1.5, y=2.5, z=3.5)
    elif msg_type == 'geometry_msgs/msg/Vector3':
        return msg_class(x=0.1, y=0.2, z=0.3)
    elif msg_type == 'geometry_msgs/msg/Quaternion':
        return msg_class(x=0.0, y=0.0, z=0.0, w=1.0)
    elif msg_type == 'geometry_msgs/msg/Pose':
        return msg_class(
            position=geometry_msgs__msg__Point(x=1.0, y=2.0, z=3.0),
            orientation=geometry_msgs__msg__Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
        )
    elif msg_type == 'geometry_msgs/msg/Pose2D':
        return msg_class(x=1.0, y=2.0, theta=0.5)
    elif msg_type == 'geometry_msgs/msg/Transform':
        return msg_class(
            translation=geometry_msgs__msg__Vector3(x=1.0, y=2.0, z=3.0),
            rotation=geometry_msgs__msg__Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
        )
    elif msg_type == 'geometry_msgs/msg/Twist':
        return msg_class(
            linear=geometry_msgs__msg__Vector3(x=1.0, y=0.0, z=0.0),
            angular=geometry_msgs__msg__Vector3(x=0.0, y=0.0, z=0.5),
        )
    elif msg_type == 'geometry_msgs/msg/Accel':
        return msg_class(
            linear=geometry_msgs__msg__Vector3(x=0.1, y=0.2, z=9.8),
            angular=geometry_msgs__msg__Vector3(x=0.0, y=0.0, z=0.1),
        )
    elif msg_type == 'geometry_msgs/msg/Wrench':
        return msg_class(
            force=geometry_msgs__msg__Vector3(x=10.0, y=5.0, z=2.0),
            torque=geometry_msgs__msg__Vector3(x=0.1, y=0.2, z=0.3),
        )
    elif msg_type == 'geometry_msgs/msg/Inertia':
        return msg_class(
            m=1.5,
            com=geometry_msgs__msg__Vector3(x=0.0, y=0.0, z=0.0),
            ixx=0.1,
            ixy=0.0,
            ixz=0.0,
            iyy=0.1,
            iyz=0.0,
            izz=0.1,
        )

    # Stamped geometry messages
    elif msg_type == 'geometry_msgs/msg/PointStamped':
        return msg_class(header=create_header(), point=geometry_msgs__msg__Point(x=1.0, y=2.0, z=3.0))
    elif msg_type == 'geometry_msgs/msg/PoseStamped':
        return msg_class(
            header=create_header(),
            pose=geometry_msgs__msg__Pose(
                position=geometry_msgs__msg__Point(x=1.0, y=2.0, z=3.0),
                orientation=geometry_msgs__msg__Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
            ),
        )
    elif msg_type == 'geometry_msgs/msg/TransformStamped':
        return msg_class(
            header=create_header(),
            child_frame_id='child_frame',
            transform=geometry_msgs__msg__Transform(
                translation=geometry_msgs__msg__Vector3(x=1.0, y=2.0, z=3.0),
                rotation=geometry_msgs__msg__Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
            ),
        )
    elif msg_type == 'geometry_msgs/msg/TwistStamped':
        return msg_class(
            header=create_header(),
            twist=geometry_msgs__msg__Twist(
                linear=geometry_msgs__msg__Vector3(x=1.0, y=0.0, z=0.0),
                angular=geometry_msgs__msg__Vector3(x=0.0, y=0.0, z=0.5),
            ),
        )
    elif msg_type == 'geometry_msgs/msg/AccelStamped':
        return msg_class(
            header=create_header(),
            accel=geometry_msgs__msg__Accel(
                linear=geometry_msgs__msg__Vector3(x=0.1, y=0.2, z=9.8),
                angular=geometry_msgs__msg__Vector3(x=0.0, y=0.0, z=0.1),
            ),
        )
    elif msg_type == 'geometry_msgs/msg/WrenchStamped':
        return msg_class(
            header=create_header(),
            wrench=geometry_msgs__msg__Wrench(
                force=geometry_msgs__msg__Vector3(x=10.0, y=5.0, z=2.0),
                torque=geometry_msgs__msg__Vector3(x=0.1, y=0.2, z=0.3),
            ),
        )
    elif msg_type == 'geometry_msgs/msg/Vector3Stamped':
        return msg_class(header=create_header(), vector=geometry_msgs__msg__Vector3(x=1.0, y=2.0, z=3.0))
    elif msg_type == 'geometry_msgs/msg/QuaternionStamped':
        return msg_class(header=create_header(), quaternion=geometry_msgs__msg__Quaternion(x=0.0, y=0.0, z=0.0, w=1.0))
    elif msg_type == 'geometry_msgs/msg/InertiaStamped':
        return msg_class(
            header=create_header(),
            inertia=geometry_msgs__msg__Inertia(
                m=1.5,
                com=geometry_msgs__msg__Vector3(x=0.0, y=0.0, z=0.0),
                ixx=0.1,
                ixy=0.0,
                ixz=0.0,
                iyy=0.1,
                iyz=0.0,
                izz=0.1,
            ),
        )
    elif msg_type == 'geometry_msgs/msg/Polygon':
        return msg_class(
            points=[
                geometry_msgs__msg__Point32(x=0.0, y=0.0, z=0.0),
                geometry_msgs__msg__Point32(x=1.0, y=0.0, z=0.0),
                geometry_msgs__msg__Point32(x=1.0, y=1.0, z=0.0),
                geometry_msgs__msg__Point32(x=0.0, y=1.0, z=0.0),
            ]
        )
    elif msg_type == 'geometry_msgs/msg/PolygonStamped':
        return msg_class(
            header=create_header(),
            polygon=geometry_msgs__msg__Polygon(
                points=[
                    geometry_msgs__msg__Point32(x=0.0, y=0.0, z=0.0),
                    geometry_msgs__msg__Point32(x=1.0, y=0.0, z=0.0),
                    geometry_msgs__msg__Point32(x=1.0, y=1.0, z=0.0),
                    geometry_msgs__msg__Point32(x=0.0, y=1.0, z=0.0),
                ]
            ),
        )
    elif msg_type == 'geometry_msgs/msg/PoseArray':
        return msg_class(
            header=create_header(),
            poses=[
                geometry_msgs__msg__Pose(
                    position=geometry_msgs__msg__Point(x=0.0, y=0.0, z=0.0),
                    orientation=geometry_msgs__msg__Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
                ),
                geometry_msgs__msg__Pose(
                    position=geometry_msgs__msg__Point(x=1.0, y=1.0, z=0.0),
                    orientation=geometry_msgs__msg__Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
                ),
            ],
        )
    elif msg_type == 'geometry_msgs/msg/PoseWithCovariance':
        return msg_class(
            pose=geometry_msgs__msg__Pose(
                position=geometry_msgs__msg__Point(x=1.0, y=2.0, z=3.0),
                orientation=geometry_msgs__msg__Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
            ),
            covariance=np.array([0.1] * 36, dtype=np.float64),
        )
    elif msg_type == 'geometry_msgs/msg/PoseWithCovarianceStamped':
        return msg_class(
            header=create_header(),
            pose=geometry_msgs__msg__PoseWithCovariance(
                pose=geometry_msgs__msg__Pose(
                    position=geometry_msgs__msg__Point(x=1.0, y=2.0, z=3.0),
                    orientation=geometry_msgs__msg__Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
                ),
                covariance=np.array([0.1] * 36, dtype=np.float64),
            ),
        )
    elif msg_type == 'geometry_msgs/msg/TwistWithCovariance':
        return msg_class(
            twist=geometry_msgs__msg__Twist(
                linear=geometry_msgs__msg__Vector3(x=1.0, y=0.0, z=0.0),
                angular=geometry_msgs__msg__Vector3(x=0.0, y=0.0, z=0.5),
            ),
            covariance=np.array([0.01] * 36, dtype=np.float64),
        )
    elif msg_type == 'geometry_msgs/msg/TwistWithCovarianceStamped':
        return msg_class(
            header=create_header(),
            twist=geometry_msgs__msg__TwistWithCovariance(
                twist=geometry_msgs__msg__Twist(
                    linear=geometry_msgs__msg__Vector3(x=1.0, y=0.0, z=0.0),
                    angular=geometry_msgs__msg__Vector3(x=0.0, y=0.0, z=0.5),
                ),
                covariance=np.array([0.01] * 36, dtype=np.float64),
            ),
        )
    elif msg_type == 'geometry_msgs/msg/AccelWithCovariance':
        return msg_class(
            accel=geometry_msgs__msg__Accel(
                linear=geometry_msgs__msg__Vector3(x=0.1, y=0.2, z=9.8),
                angular=geometry_msgs__msg__Vector3(x=0.0, y=0.0, z=0.1),
            ),
            covariance=np.array([0.1] * 36, dtype=np.float64),
        )
    elif msg_type == 'geometry_msgs/msg/AccelWithCovarianceStamped':
        return msg_class(
            header=create_header(),
            accel=geometry_msgs__msg__AccelWithCovariance(
                accel=geometry_msgs__msg__Accel(
                    linear=geometry_msgs__msg__Vector3(x=0.1, y=0.2, z=9.8),
                    angular=geometry_msgs__msg__Vector3(x=0.0, y=0.0, z=0.1),
                ),
                covariance=np.array([0.1] * 36, dtype=np.float64),
            ),
        )

    # Default fallback - try to create with minimal parameters
    else:
        try:
            return msg_class()
        except Exception:
            # For complex messages, we'll need specific handling
            return create_complex_message(msg_class, msg_type)


def create_complex_message(msg_class: Any, msg_type: str) -> Any:
    """Create complex messages that require specific field initialization."""

    # Sensor messages
    if msg_type == 'sensor_msgs/msg/LaserScan':
        return msg_class(
            header=create_header(),
            angle_min=-1.57,
            angle_max=1.57,
            angle_increment=0.01,
            time_increment=0.0,
            scan_time=0.1,
            range_min=0.1,
            range_max=10.0,
            ranges=np.array([1.0, 2.0, 3.0, 4.0, 5.0], dtype=np.float32),
            intensities=np.array([100.0, 200.0, 300.0, 400.0, 500.0], dtype=np.float32),
        )
    elif msg_type == 'sensor_msgs/msg/Image':
        return msg_class(
            header=create_header(),
            height=480,
            width=640,
            encoding='rgb8',
            is_bigendian=0,
            step=1920,  # width * 3 for rgb8
            data=np.array([255, 0, 0] * 10, dtype=np.uint8),  # Small red pixel data
        )
    elif msg_type == 'sensor_msgs/msg/PointCloud':
        return msg_class(
            header=create_header(),
            points=np.array(
                [
                    geometry_msgs__msg__Point32(x=1.0, y=2.0, z=3.0),
                    geometry_msgs__msg__Point32(x=4.0, y=5.0, z=6.0),
                    geometry_msgs__msg__Point32(x=7.0, y=8.0, z=9.0),
                ]
            ),
            channels=np.array(
                [
                    sensor_msgs__msg__ChannelFloat32(
                        name='intensity', values=np.array([100.0, 200.0, 300.0], dtype=np.float32)
                    )
                ]
            ),
        )
    elif msg_type == 'sensor_msgs/msg/PointCloud2':
        return msg_class(
            header=create_header(),
            height=1,
            width=3,
            fields=[
                sensor_msgs__msg__PointField(name='x', offset=0, datatype=7, count=1),
                sensor_msgs__msg__PointField(name='y', offset=4, datatype=7, count=1),
                sensor_msgs__msg__PointField(name='z', offset=8, datatype=7, count=1),
            ],
            is_bigendian=False,
            point_step=12,
            row_step=36,
            data=np.array([0] * 36, dtype=np.uint8),
            is_dense=True,
        )
    elif msg_type == 'sensor_msgs/msg/JointState':
        return msg_class(
            header=create_header(),
            name=['joint1', 'joint2', 'joint3'],
            position=np.array([0.1, 0.2, 0.3], dtype=np.float64),
            velocity=np.array([0.01, 0.02, 0.03], dtype=np.float64),
            effort=np.array([1.0, 2.0, 3.0], dtype=np.float64),
        )
    elif msg_type == 'sensor_msgs/msg/Imu':
        return msg_class(
            header=create_header(),
            orientation=geometry_msgs__msg__Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
            orientation_covariance=np.array([0.1] * 9, dtype=np.float64),
            angular_velocity=geometry_msgs__msg__Vector3(x=0.01, y=0.02, z=0.03),
            angular_velocity_covariance=np.array([0.01] * 9, dtype=np.float64),
            linear_acceleration=geometry_msgs__msg__Vector3(x=0.1, y=0.2, z=9.8),
            linear_acceleration_covariance=np.array([0.1] * 9, dtype=np.float64),
        )
    elif msg_type == 'sensor_msgs/msg/Range':
        return msg_class(
            header=create_header(),
            radiation_type=0,  # ULTRASOUND
            field_of_view=0.1,
            min_range=0.02,
            max_range=2.0,
            range=1.5,
        )
    elif msg_type == 'sensor_msgs/msg/Temperature':
        return msg_class(header=create_header(), temperature=25.5, variance=0.1)
    elif msg_type == 'sensor_msgs/msg/FluidPressure':
        return msg_class(header=create_header(), fluid_pressure=101325.0, variance=100.0)
    elif msg_type == 'sensor_msgs/msg/Illuminance':
        return msg_class(header=create_header(), illuminance=500.0, variance=10.0)
    elif msg_type == 'sensor_msgs/msg/MagneticField':
        return msg_class(
            header=create_header(),
            magnetic_field=geometry_msgs__msg__Vector3(x=0.1, y=0.2, z=0.3),
            magnetic_field_covariance=np.array([0.01] * 9, dtype=np.float64),
        )
    elif msg_type == 'sensor_msgs/msg/NavSatFix':
        return msg_class(
            header=create_header(),
            status=sensor_msgs__msg__NavSatStatus(status=0, service=1),
            latitude=37.7749,
            longitude=-122.4194,
            altitude=100.0,
            position_covariance=np.array([1.0] * 9, dtype=np.float64),
            position_covariance_type=1,
        )
    elif msg_type == 'sensor_msgs/msg/BatteryState':
        return msg_class(
            header=create_header(),
            voltage=12.6,
            temperature=25.0,
            current=-5.0,
            charge=50.0,
            capacity=100.0,
            design_capacity=100.0,
            percentage=0.5,
            power_supply_status=2,  # DISCHARGING
            power_supply_health=1,  # GOOD
            power_supply_technology=1,  # LION
            present=True,
            cell_voltage=np.array([3.7, 3.7, 3.7], dtype=np.float32),
            cell_temperature=np.array([25.0, 25.0, 25.0], dtype=np.float32),
            location='battery_compartment',
            serial_number='BAT123456',
        )
    elif msg_type == 'sensor_msgs/msg/CameraInfo':
        return msg_class(
            header=create_header(),
            height=480,
            width=640,
            distortion_model='plumb_bob',
            d=np.array([0.1, -0.2, 0.001, 0.002, 0.0], dtype=np.float64),
            k=np.array([525.0, 0.0, 320.0, 0.0, 525.0, 240.0, 0.0, 0.0, 1.0], dtype=np.float64),
            r=np.array([1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0], dtype=np.float64),
            p=np.array([525.0, 0.0, 320.0, 0.0, 0.0, 525.0, 240.0, 0.0, 0.0, 0.0, 1.0, 0.0], dtype=np.float64),
            binning_x=1,
            binning_y=1,
            roi=sensor_msgs__msg__RegionOfInterest(x_offset=0, y_offset=0, height=0, width=0, do_rectify=False),
        )
    elif msg_type == 'sensor_msgs/msg/ChannelFloat32':
        return msg_class(
            name='intensity',
            values=np.array([100.0, 200.0, 300.0], dtype=np.float32),
        )
    elif msg_type == 'sensor_msgs/msg/CompressedImage':
        return msg_class(
            header=create_header(),
            format='jpeg',
            data=np.array([255, 216, 255, 224, 0, 16, 74, 70, 73, 70], dtype=np.uint8),  # JPEG header
        )
    elif msg_type == 'sensor_msgs/msg/Joy':
        return msg_class(
            header=create_header(),
            axes=np.array([0.0, 0.5, -0.3, 1.0], dtype=np.float32),
            buttons=np.array([0, 1, 0, 1, 0], dtype=np.int32),
        )
    elif msg_type == 'sensor_msgs/msg/JoyFeedback':
        return msg_class(
            type=1,  # TYPE_LED
            id=0,
            intensity=0.8,
        )
    elif msg_type == 'sensor_msgs/msg/JoyFeedbackArray':
        return msg_class(
            array=[
                sensor_msgs__msg__JoyFeedback(type=1, id=0, intensity=0.8),
                sensor_msgs__msg__JoyFeedback(type=2, id=1, intensity=0.5),
            ]
        )
    elif msg_type == 'sensor_msgs/msg/LaserEcho':
        return msg_class(
            echoes=np.array([1.5, 2.0, 2.5], dtype=np.float32),
        )
    elif msg_type == 'sensor_msgs/msg/MultiDOFJointState':
        return msg_class(
            header=create_header(),
            joint_names=['joint1', 'joint2'],
            transforms=[
                geometry_msgs__msg__Transform(
                    translation=geometry_msgs__msg__Vector3(x=1.0, y=0.0, z=0.0),
                    rotation=geometry_msgs__msg__Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
                ),
                geometry_msgs__msg__Transform(
                    translation=geometry_msgs__msg__Vector3(x=0.0, y=1.0, z=0.0),
                    rotation=geometry_msgs__msg__Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
                ),
            ],
            twist=[
                geometry_msgs__msg__Twist(
                    linear=geometry_msgs__msg__Vector3(x=0.1, y=0.0, z=0.0),
                    angular=geometry_msgs__msg__Vector3(x=0.0, y=0.0, z=0.01),
                ),
                geometry_msgs__msg__Twist(
                    linear=geometry_msgs__msg__Vector3(x=0.0, y=0.1, z=0.0),
                    angular=geometry_msgs__msg__Vector3(x=0.0, y=0.0, z=0.01),
                ),
            ],
            wrench=[
                geometry_msgs__msg__Wrench(
                    force=geometry_msgs__msg__Vector3(x=1.0, y=0.0, z=0.0),
                    torque=geometry_msgs__msg__Vector3(x=0.0, y=0.0, z=0.1),
                ),
                geometry_msgs__msg__Wrench(
                    force=geometry_msgs__msg__Vector3(x=0.0, y=1.0, z=0.0),
                    torque=geometry_msgs__msg__Vector3(x=0.0, y=0.0, z=0.1),
                ),
            ],
        )
    elif msg_type == 'sensor_msgs/msg/MultiEchoLaserScan':
        return msg_class(
            header=create_header(),
            angle_min=-1.57,
            angle_max=1.57,
            angle_increment=0.1,
            time_increment=0.001,
            scan_time=0.1,
            range_min=0.1,
            range_max=10.0,
            ranges=np.array(
                [
                    sensor_msgs__msg__LaserEcho(echoes=np.array([1.5, 2.0], dtype=np.float32)),
                    sensor_msgs__msg__LaserEcho(echoes=np.array([2.5, 3.0], dtype=np.float32)),
                    sensor_msgs__msg__LaserEcho(echoes=np.array([3.5, 4.0], dtype=np.float32)),
                ]
            ),
            intensities=np.array(
                [
                    sensor_msgs__msg__LaserEcho(echoes=np.array([100.0, 150.0], dtype=np.float32)),
                    sensor_msgs__msg__LaserEcho(echoes=np.array([200.0, 250.0], dtype=np.float32)),
                    sensor_msgs__msg__LaserEcho(echoes=np.array([300.0, 350.0], dtype=np.float32)),
                ]
            ),
        )
    elif msg_type == 'sensor_msgs/msg/NavSatStatus':
        return msg_class(
            status=0,  # STATUS_NO_FIX
            service=1,  # SERVICE_GPS
        )
    elif msg_type == 'sensor_msgs/msg/PointField':
        return msg_class(
            name='x',
            offset=0,
            datatype=7,  # FLOAT32
            count=1,
        )
    elif msg_type == 'sensor_msgs/msg/RegionOfInterest':
        return msg_class(
            x_offset=10,
            y_offset=20,
            height=100,
            width=200,
            do_rectify=False,
        )
    elif msg_type == 'sensor_msgs/msg/RelativeHumidity':
        return msg_class(
            header=create_header(),
            relative_humidity=0.65,  # 65%
            variance=0.01,
        )
    elif msg_type == 'sensor_msgs/msg/TimeReference':
        return msg_class(
            header=create_header(),
            time_ref=builtin_interfaces__msg__Time(sec=1234567890, nanosec=123456789),
            source='gps',
        )

    # Nav messages
    elif msg_type == 'nav_msgs/msg/Odometry':
        return msg_class(
            header=create_header(),
            child_frame_id='base_link',
            pose=geometry_msgs__msg__PoseWithCovariance(
                pose=geometry_msgs__msg__Pose(
                    position=geometry_msgs__msg__Point(x=1.0, y=2.0, z=0.0),
                    orientation=geometry_msgs__msg__Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
                ),
                covariance=np.array([0.1] * 36, dtype=np.float64),
            ),
            twist=geometry_msgs__msg__TwistWithCovariance(
                twist=geometry_msgs__msg__Twist(
                    linear=geometry_msgs__msg__Vector3(x=1.0, y=0.0, z=0.0),
                    angular=geometry_msgs__msg__Vector3(x=0.0, y=0.0, z=0.1),
                ),
                covariance=np.array([0.01] * 36, dtype=np.float64),
            ),
        )
    elif msg_type == 'nav_msgs/msg/Path':
        return msg_class(
            header=create_header(),
            poses=[
                geometry_msgs__msg__PoseStamped(
                    header=create_header(),
                    pose=geometry_msgs__msg__Pose(
                        position=geometry_msgs__msg__Point(x=0.0, y=0.0, z=0.0),
                        orientation=geometry_msgs__msg__Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
                    ),
                ),
                geometry_msgs__msg__PoseStamped(
                    header=create_header(),
                    pose=geometry_msgs__msg__Pose(
                        position=geometry_msgs__msg__Point(x=1.0, y=1.0, z=0.0),
                        orientation=geometry_msgs__msg__Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
                    ),
                ),
            ],
        )
    elif msg_type == 'nav_msgs/msg/OccupancyGrid':
        return msg_class(
            header=create_header(),
            info=nav_msgs__msg__MapMetaData(
                map_load_time=builtin_interfaces__msg__Time(sec=0, nanosec=0),
                resolution=0.05,
                width=10,
                height=10,
                origin=geometry_msgs__msg__Pose(
                    position=geometry_msgs__msg__Point(x=0.0, y=0.0, z=0.0),
                    orientation=geometry_msgs__msg__Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
                ),
            ),
            data=np.array([-1] * 100, dtype=np.int8),  # 10x10 grid with unknown values
        )
    elif msg_type == 'nav_msgs/msg/GridCells':
        return msg_class(
            header=create_header(),
            cell_width=0.1,
            cell_height=0.1,
            cells=[
                geometry_msgs__msg__Point(x=1.0, y=1.0, z=0.0),
                geometry_msgs__msg__Point(x=2.0, y=2.0, z=0.0),
                geometry_msgs__msg__Point(x=3.0, y=3.0, z=0.0),
            ],
        )
    elif msg_type == 'nav_msgs/msg/MapMetaData':
        return msg_class(
            map_load_time=builtin_interfaces__msg__Time(sec=0, nanosec=0),
            resolution=0.05,
            width=10,
            height=10,
            origin=geometry_msgs__msg__Pose(
                position=geometry_msgs__msg__Point(x=0.0, y=0.0, z=0.0),
                orientation=geometry_msgs__msg__Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
            ),
        )

    # Multi-array messages
    elif msg_type == 'std_msgs/msg/Float32MultiArray':
        return msg_class(
            layout=std_msgs__msg__MultiArrayLayout(
                dim=[std_msgs__msg__MultiArrayDimension(label='x', size=3, stride=3)], data_offset=0
            ),
            data=np.array([1.1, 2.2, 3.3], dtype=np.float32),
        )
    elif msg_type == 'std_msgs/msg/Int32MultiArray':
        return msg_class(
            layout=std_msgs__msg__MultiArrayLayout(
                dim=[std_msgs__msg__MultiArrayDimension(label='x', size=3, stride=3)], data_offset=0
            ),
            data=np.array([1, 2, 3], dtype=np.int32),
        )
    elif msg_type == 'std_msgs/msg/Int64MultiArray':
        return msg_class(
            layout=std_msgs__msg__MultiArrayLayout(
                dim=[std_msgs__msg__MultiArrayDimension(label='x', size=3, stride=3)], data_offset=0
            ),
            data=np.array([100, 200, 300], dtype=np.int64),
        )
    elif msg_type == 'std_msgs/msg/UInt64MultiArray':
        return msg_class(
            layout=std_msgs__msg__MultiArrayLayout(
                dim=[std_msgs__msg__MultiArrayDimension(label='x', size=3, stride=3)], data_offset=0
            ),
            data=np.array([1000, 2000, 3000], dtype=np.uint64),
        )
    elif msg_type == 'std_msgs/msg/UInt32MultiArray':
        return msg_class(
            layout=std_msgs__msg__MultiArrayLayout(
                dim=[std_msgs__msg__MultiArrayDimension(label='x', size=3, stride=3)], data_offset=0
            ),
            data=np.array([100, 200, 300], dtype=np.uint32),
        )
    elif msg_type == 'std_msgs/msg/UInt16MultiArray':
        return msg_class(
            layout=std_msgs__msg__MultiArrayLayout(
                dim=[std_msgs__msg__MultiArrayDimension(label='x', size=3, stride=3)], data_offset=0
            ),
            data=np.array([10, 20, 30], dtype=np.uint16),
        )
    elif msg_type == 'std_msgs/msg/UInt8MultiArray':
        return msg_class(
            layout=std_msgs__msg__MultiArrayLayout(
                dim=[std_msgs__msg__MultiArrayDimension(label='x', size=3, stride=3)], data_offset=0
            ),
            data=np.array([1, 2, 3], dtype=np.uint8),
        )
    elif msg_type == 'std_msgs/msg/Int16MultiArray':
        return msg_class(
            layout=std_msgs__msg__MultiArrayLayout(
                dim=[std_msgs__msg__MultiArrayDimension(label='x', size=3, stride=3)], data_offset=0
            ),
            data=np.array([10, 20, 30], dtype=np.int16),
        )
    elif msg_type == 'std_msgs/msg/Int8MultiArray':
        return msg_class(
            layout=std_msgs__msg__MultiArrayLayout(
                dim=[std_msgs__msg__MultiArrayDimension(label='x', size=3, stride=3)], data_offset=0
            ),
            data=np.array([1, 2, 3], dtype=np.int8),
        )
    elif msg_type == 'std_msgs/msg/Float64MultiArray':
        return msg_class(
            layout=std_msgs__msg__MultiArrayLayout(
                dim=[std_msgs__msg__MultiArrayDimension(label='x', size=3, stride=3)], data_offset=0
            ),
            data=np.array([1.11, 2.22, 3.33], dtype=np.float64),
        )
    elif msg_type == 'std_msgs/msg/ByteMultiArray':
        return msg_class(
            layout=std_msgs__msg__MultiArrayLayout(
                dim=[std_msgs__msg__MultiArrayDimension(label='x', size=3, stride=3)], data_offset=0
            ),
            data=np.array([65, 66, 67], dtype=np.uint8),  # 'A', 'B', 'C'
        )
    elif msg_type == 'tf2_msgs/msg/TF2Error':
        return msg_class(
            error=0,  # NO_ERROR
            error_string='No error',
        )
    elif msg_type == 'tf2_msgs/msg/TFMessage':
        return msg_class(
            transforms=[
                geometry_msgs__msg__TransformStamped(
                    header=create_header(),
                    child_frame_id='child_frame',
                    transform=geometry_msgs__msg__Transform(
                        translation=geometry_msgs__msg__Vector3(x=1.0, y=2.0, z=3.0),
                        rotation=geometry_msgs__msg__Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
                    ),
                )
            ]
        )
    elif msg_type == 'stereo_msgs/msg/DisparityImage':
        return msg_class(
            header=create_header(),
            image=sensor_msgs__msg__Image(
                header=create_header(),
                height=480,
                width=640,
                encoding='32FC1',
                is_bigendian=0,
                step=2560,  # width * 4 for 32FC1
                data=np.array([1.0] * 100, dtype=np.float32).view(np.uint8),  # Disparity data as bytes
            ),
            f=525.0,  # focal length
            t=0.1,  # baseline
            valid_window=sensor_msgs__msg__RegionOfInterest(
                x_offset=0,
                y_offset=0,
                height=480,
                width=640,
                do_rectify=False,
            ),
            min_disparity=0.0,
            max_disparity=64.0,
            delta_d=0.125,
        )

    # Default fallback for any remaining complex types
    else:
        # Try to create with empty constructor
        return msg_class()


def select_all_message_types() -> List[Tuple[str, Any, str]]:
    """Select all available message types for comprehensive testing."""
    return [(msg_type, MESSAGE_TYPES[msg_type][0], MESSAGE_TYPES[msg_type][1]) for msg_type in MESSAGE_TYPES.keys()]


def fix_metadata_for_humble(bag_path: str) -> None:
    """Fix metadata.yaml to be compatible with ROS2 Humble."""
    import yaml

    metadata_path = Path(bag_path) / 'metadata.yaml'
    if not metadata_path.exists():
        return

    # Load the metadata
    with metadata_path.open('r') as f:
        data = yaml.safe_load(f)

    if 'rosbag2_bagfile_information' in data:
        bag_info = data['rosbag2_bagfile_information']

        # Change version to 5 for Humble compatibility
        bag_info['version'] = 5

        # Remove fields that are not supported in Humble
        if 'ros_distro' in bag_info:
            del bag_info['ros_distro']
        if 'custom_data' in bag_info:
            del bag_info['custom_data']

        # Fix topics metadata
        if 'topics_with_message_count' in bag_info:
            for topic_info in bag_info['topics_with_message_count']:
                if 'topic_metadata' in topic_info:
                    topic_meta = topic_info['topic_metadata']

                    # Remove type_description_hash (added in version 7)
                    if 'type_description_hash' in topic_meta:
                        del topic_meta['type_description_hash']

                    # Convert offered_qos_profiles from array to string format for version 5
                    if 'offered_qos_profiles' in topic_meta:
                        qos = topic_meta['offered_qos_profiles']
                        if isinstance(qos, list) and len(qos) == 0:
                            topic_meta['offered_qos_profiles'] = ''

    # Write back the fixed metadata
    with metadata_path.open('w') as f:
        yaml.safe_dump(data, f, default_flow_style=False)


def create_bag_file(bag_path: str, storage_format: str, selected_types: List[Tuple[str, Any, str]]) -> None:
    """Create a bag file with the specified format and message types."""
    typestore = get_typestore(Stores.ROS2_HUMBLE)

    # Determine storage plugin based on format
    if storage_format.lower() == 'sqlite3':
        storage_plugin = StoragePlugin.SQLITE3
    elif storage_format.lower() == 'mcap':
        storage_plugin = StoragePlugin.MCAP
    else:
        error_msg = f'Unsupported storage format: {storage_format}'
        raise ValueError(error_msg)

    # Use version 8 for ROS2 Humble compatibility (version 5 not supported by rosbags)
    with Writer(bag_path, version=8, storage_plugin=storage_plugin) as writer:
        connections = []

        # Add connections for each selected message type
        for msg_type, msg_class, topic_name in selected_types:
            connection = writer.add_connection(topic_name, msg_type, typestore=typestore)
            connections.append((connection, msg_class, msg_type, topic_name))

        # Write 2 messages per topic
        base_timestamp = int(time.time() * 1e9)
        message_count = 0

        for i in range(2):  # 2 messages per topic
            for connection, msg_class, msg_type, topic_name in connections:
                # Create sample message
                message = create_sample_message(msg_class, msg_type)

                # Serialize and write
                timestamp = base_timestamp + (message_count * 100_000_000)  # 100ms intervals
                try:
                    serialized_data = typestore.serialize_cdr(message, msg_type)
                    writer.write(connection, timestamp, serialized_data)
                    message_count += 1
                    print(f'  Written message {i + 1}/2 for {topic_name} ({msg_type})')
                except Exception as e:
                    print(f'  ERROR: Failed to serialize {msg_type}: {e}')
                    print(f'  Message content: {message}')
                    raise

    # Fix metadata for ROS2 Humble compatibility
    fix_metadata_for_humble(bag_path)


def test_bag_compatibility() -> None:
    """Test the generated bag files with ros2 bag info command."""
    import os
    import subprocess

    # Source ROS2 Humble environment and test bags
    ros_setup = 'source /opt/ros/humble/setup.zsh'

    for bag_name in ['test_bag_sqlite3', 'test_bag_mcap']:
        print(f'  Testing {bag_name}...')
        try:
            # Run ros2 bag info command
            cmd = f'{ros_setup} && ros2 bag info {bag_name}'
            result = subprocess.run(cmd, shell=True, executable='/bin/zsh', capture_output=True, text=True, timeout=10)

            if result.returncode == 0:
                print(f'    ✓ {bag_name} is compatible with ros2 bag info')
                # Print first few lines of output to show it worked
                lines = result.stdout.strip().split('\n')[:3]
                for line in lines:
                    print(f'      {line}')
            else:
                print(f'    ✗ {bag_name} failed ros2 bag info test')
                print(f'      Error: {result.stderr.strip()}')

        except subprocess.TimeoutExpired:
            print(f'    ✗ {bag_name} test timed out')
        except Exception as e:
            print(f'    ✗ {bag_name} test failed: {e}')


def main():
    """Main function to generate test bag files."""
    print('Generating comprehensive test ROS2 bag files with ALL supported message types...')

    # Select all available message types for comprehensive testing
    selected_types = select_all_message_types()

    print(f'\nIncluding ALL {len(selected_types)} supported message types:')
    print('  (Showing first 10 for brevity)')
    for msg_type, _, topic_name in selected_types[:10]:
        print(f'  - {msg_type} -> {topic_name}')
    if len(selected_types) > 10:
        print(f'  ... and {len(selected_types) - 10} more message types')

    # Create SQLite3 bag
    print(f'\nCreating SQLite3 bag file...')
    create_bag_file('test_bag_sqlite3', 'sqlite3', selected_types)
    print(f'✓ Created test_bag_sqlite3.db3')

    # Create MCAP bag
    print(f'\nCreating MCAP bag file...')
    create_bag_file('test_bag_mcap', 'mcap', selected_types)
    print(f'✓ Created test_bag_mcap.mcap')

    print(f'\n✓ Successfully generated both bag files with {len(selected_types)} message types')
    print(f'  Total messages written: {len(selected_types) * 2} (2 per topic)')

    # Test the generated bags with ros2 bag info
    print(f'\nTesting bag files with ros2 bag info...')
    test_bag_compatibility()


if __name__ == '__main__':
    main()
