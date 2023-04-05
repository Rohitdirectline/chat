windows_conf = {
    "in_radar_dataview_driver_claim": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "dataviews_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["dataviews_id"],
            "source": "payload-root-Quote.DataViews",
            "hierarchy": 1,
        },
        "dv_policy_id": {
            "partitionBy": ["quote_id", "dataviews_id"],
            "orderBy": ["dv_policy_id"],
            "source": "payload-root-Quote-DataViews.Policy",
            "hierarchy": 2,
        },
        "dv_driver_id": {
            "partitionBy": ["quote_id", "dataviews_id", "dv_policy_id"],
            "orderBy": ["dv_driver_id"],
            "source": "payload-root-Quote-DataViews-Policy.Driver",
            "hierarchy": 3,
        },
        "dv_driver_claim_id": {
            "partitionBy": [
                "quote_id",
                "dataviews_id",
                "dv_policy_id",
                "dv_driver_id",
            ],
            "orderBy": ["dv_driver_claim_id"],
            "source": "payload-root-Quote-DataViews-Policy-Driver.Claim",
            "hierarchy": 4,
        },
    },
    "in_radar_dataview_driver_conv": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "dataviews_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["dataviews_id"],
            "source": "payload-root-Quote.DataViews",
            "hierarchy": 1,
        },
        "dv_policy_id": {
            "partitionBy": ["quote_id", "dataviews_id"],
            "orderBy": ["dv_policy_id"],
            "source": "payload-root-Quote-DataViews.Policy",
            "hierarchy": 2,
        },
        "dv_driver_id": {
            "partitionBy": ["quote_id", "dataviews_id", "dv_policy_id"],
            "orderBy": ["dv_driver_id"],
            "source": "payload-root-Quote-DataViews-Policy.Driver",
            "hierarchy": 3,
        },
        "dv_driver_conviction_id": {
            "partitionBy": [
                "quote_id",
                "dataviews_id",
                "dv_policy_id",
                "dv_driver_id",
            ],
            "orderBy": ["dv_driver_conviction_id"],
            "source": "payload-root-Quote-DataViews-Policy-Driver.Conviction",
            "hierarchy": 4,
        },
    },
    "in_radar_dataview_driver": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "dataviews_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["dataviews_id"],
            "source": "payload-root-Quote.DataViews",
            "hierarchy": 1,
        },
        "dv_policy_id": {
            "partitionBy": ["quote_id", "dataviews_id"],
            "orderBy": ["dv_policy_id"],
            "source": "payload-root-Quote-DataViews.Policy",
            "hierarchy": 2,
        },
        "dv_driver_id": {
            "partitionBy": ["quote_id", "dataviews_id", "dv_policy_id"],
            "orderBy": ["dv_driver_id"],
            "source": "payload-root-Quote-DataViews-Policy.Driver",
            "hierarchy": 3,
        },
    },
    "in_radar_dataview_driveronveh": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "dataviews_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["dataviews_id"],
            "source": "payload-root-Quote.DataViews",
            "hierarchy": 1,
        },
        "dv_policy_id": {
            "partitionBy": ["quote_id", "dataviews_id"],
            "orderBy": ["dv_policy_id"],
            "source": "payload-root-Quote-DataViews.Policy",
            "hierarchy": 2,
        },
        "dv_vehicle_id": {
            "partitionBy": ["quote_id", "dataviews_id", "dv_policy_id"],
            "orderBy": ["dv_vehicle_id"],
            "source": "payload-root-Quote-DataViews-Policy.Vehicle",
            "hierarchy": 3,
        },
        "dv_driver_on_veh_id": {
            "partitionBy": [
                "quote_id",
                "dataviews_id",
                "dv_policy_id",
                "dv_vehicle_id",
            ],
            "orderBy": ["dv_driver_on_veh_id"],
            "source": "payload-root-Quote-DataViews-Policy-Vehicle.DriverOnVehicle",
            "hierarchy": 4,
        },
    },
    "in_radar_dataview_policy": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "dataviews_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["dataviews_id"],
            "source": "payload-root-Quote.DataViews",
            "hierarchy": 1,
        },
        "dv_policy_id": {
            "partitionBy": ["quote_id", "dataviews_id"],
            "orderBy": ["dv_policy_id"],
            "source": "payload-root-Quote-DataViews.Policy",
            "hierarchy": 2,
        },
    },
    "in_radar_dataview_rsqclaim": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "dataviews_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["dataviews_id"],
            "source": "payload-root-Quote.DataViews",
            "hierarchy": 1,
        },
        "dv_policy_id": {
            "partitionBy": ["quote_id", "dataviews_id"],
            "orderBy": ["dv_policy_id"],
            "source": "payload-root-Quote-DataViews.Policy",
            "hierarchy": 2,
        },
        "dv_rescue_claim_id": {
            "partitionBy": ["quote_id", "dataviews_id", "dv_policy_id"],
            "orderBy": ["dv_rescue_claim_id"],
            "source": "payload-root-Quote-DataViews-Policy.RsqClaim",
            "hierarchy": 3,
        },
    },
    "in_radar_dataview_telematics": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload-root-Quote",
            "hierarchy": 0,
        },
        "dataviews_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["dataviews_id"],
            "source": "payload-root-Quote.DataViews",
            "hierarchy": 1,
        },
        "dv_policy_id": {
            "partitionBy": ["quote_id", "dataviews_id"],
            "orderBy": ["dv_policy_id"],
            "source": "payload-root-Quote-DataViews.Policy",
            "hierarchy": 2,
        },
        "dv_vehicle_id": {
            "partitionBy": ["quote_id", "dataviews_id", "dv_policy_id"],
            "orderBy": ["dv_vehicle_id"],
            "source": "payload-root-Quote-DataViews-Policy.Vehicle",
            "hierarchy": 3,
        },
        "dv_telematics_id": {
            "partitionBy": [
                "quote_id",
                "dataviews_id",
                "dv_policy_id",
                "dv_vehicle_id",
            ],
            "orderBy": ["dv_telematics_id"],
            "source": "dv_dataviews_id_ref-Policy-Vehicle.Telematics",
            "hierarchy": 4,
        },
    },
    "in_radar_dataview_veh": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "dataviews_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["dataviews_id"],
            "source": "payload-root-Quote.DataViews",
            "hierarchy": 1,
        },
        "dv_policy_id": {
            "partitionBy": ["quote_id", "dataviews_id"],
            "orderBy": ["dv_policy_id"],
            "source": "payload-root-Quote-DataViews.Policy",
            "hierarchy": 2,
        },
        "dv_vehicle_id": {
            "partitionBy": ["quote_id", "dataviews_id", "dv_policy_id"],
            "orderBy": ["dv_vehicle_id"],
            "source": "payload-root-Quote-DataViews-Policy.Vehicle",
            "hierarchy": 3,
        },
    },
    "in_radar_dataviews": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "dataviews_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["dataviews_id"],
            "source": "payload-root-Quote.DataViews",
            "hierarchy": 1,
        },
    },
    "in_radar_driver_claim": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "driver_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["driver_id"],
            "source": "payload-root-Quote.Driver",
            "hierarchy": 1,
        },
        "id": {
            "partitionBy": ["quote_id", "driver_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote-Driver.Claim",
            "hierarchy": 2,
        },
    },
    "in_radar_driver_conviction": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "driver_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["driver_id"],
            "source": "payload-root-Quote.Driver",
            "hierarchy": 1,
        },
        "id": {
            "partitionBy": ["quote_id", "driver_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote-Driver.Conviction",
            "hierarchy": 2,
        },
    },
    "in_radar_driver_mibmsg": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "driver_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["driver_id"],
            "source": "payload-root-Quote.Driver",
            "hierarchy": 1,
        },
        "driver_mib_msg_id": {
            "partitionBy": ["quote_id", "driver_id"],
            "orderBy": ["driver_mib_msg_id"],
            "source": "payload-root-Quote-Driver.MIBMessage",
            "hierarchy": 2,
        },
    },
    "in_radar_driver": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote.Driver",
            "hierarchy": 1,
        },
    },
    "in_radar_driver_on_vehicle": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "vehicle_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["vehicle_id"],
            "source": "payload-root-Quote.Vehicle",
            "hierarchy": 1,
        },
        "id": {
            "partitionBy": ["quote_id", "vehicle_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote-Vehicle.DriverOnVehicle",
            "hierarchy": 2,
        },
    },
    "in_radar_phq_claim": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote.PhCueClaim",
            "hierarchy": 1,
        },
    },
    "in_radar_quote": {
        "id": {
            "partitionBy": ["id"],
            "orderBy": ["id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
    },
    "in_radar_rsqclaim": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote.RsqClaim",
            "hierarchy": 1,
        },
    },
    "in_radar_vehicle_miaftr": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "vehicle_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["vehicle_id"],
            "source": "payload-root-Quote.Vehicle",
            "hierarchy": 1,
        },
        "id": {
            "partitionBy": ["quote_id", "vehicle_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote-Vehicle.Miaftr",
            "hierarchy": 2,
        },
    },
    "in_radar_vehicle_mot_comment": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "vehicle_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["vehicle_id"],
            "source": "payload-root-Quote.Vehicle",
            "hierarchy": 1,
        },
        "mot_id": {
            "partitionBy": ["quote_id", "vehicle_id"],
            "orderBy": ["mot_id"],
            "source": "payload-root-Quote-Vehicle.MOT",
            "hierarchy": 2,
        },
        "id": {
            "partitionBy": ["quote_id", "vehicle_id", "mot_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote-Vehicle-MOT.MOTComment",
            "hierarchy": 3,
        },
    },
    "in_radar_vehicle_mot": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "vehicle_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["vehicle_id"],
            "source": "payload-root-Quote.Vehicle",
            "hierarchy": 1,
        },
        "id": {
            "partitionBy": ["quote_id", "vehicle_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote-Vehicle.MOT",
            "hierarchy": 2,
        },
    },
    "in_radar_vehicle_producttier": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "vehicle_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["vehicle_id"],
            "source": "payload-root-Quote.Vehicle",
            "hierarchy": 1,
        },
        "id": {
            "partitionBy": ["quote_id", "vehicle_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote-Vehicle.ProductTier",
            "hierarchy": 2,
        },
    },
    "in_radar_vehicle_promo": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "vehicle_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["vehicle_id"],
            "source": "payload-root-Quote.Vehicle",
            "hierarchy": 1,
        },
        "id": {
            "partitionBy": ["quote_id", "vehicle_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote-Vehicle.Promo",
            "hierarchy": 2,
        },
    },
    "in_radar_vehicle_qm": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "vehicle_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["vehicle_id"],
            "source": "payload-root-Quote.Vehicle",
            "hierarchy": 1,
        },
        "id": {
            "partitionBy": ["quote_id", "vehicle_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote-Vehicle.QM",
            "hierarchy": 2,
        },
    },
    "in_radar_vehicle_retainer": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "vehicle_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["vehicle_id"],
            "source": "payload-root-Quote.Vehicle",
            "hierarchy": 1,
        },
        "id": {
            "partitionBy": ["quote_id", "vehicle_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote-Vehicle.Retainer",
            "hierarchy": 2,
        },
    },
    "in_radar_vehicle_telematics": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "vehicle_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["vehicle_id"],
            "source": "payload-root-Quote.Vehicle",
            "hierarchy": 1,
        },
        "id": {
            "partitionBy": ["quote_id", "vehicle_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote-Vehicle.Telematics",
            "hierarchy": 2,
        },
    },
    "in_radar_vehicle": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote.Vehicle",
            "hierarchy": 1,
        },
    },
    "out_radar_quote_decline": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote.Decline",
            "hierarchy": 1,
        },
    },
    "out_radar_vehicle_promo": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "vehicle_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["vehicle_id"],
            "source": "payload-root-Quote.Vehicle",
            "hierarchy": 1,
        },
        "id": {
            "partitionBy": ["quote_id", "vehicle_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote-Vehicle.Promo",
            "hierarchy": 2,
        },
    },
    "out_radar_motor_driver_on_vehicle": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "vehicle_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["vehicle_id"],
            "source": "payload-root-Quote.Vehicle",
            "hierarchy": 1,
        },
        "id": {
            "partitionBy": ["quote_id", "vehicle_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote-Vehicle.DriverOnVehicle",
            "hierarchy": 2,
        },
    },
    "out_radar_vehicle_retainer": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "vehicle_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["vehicle_id"],
            "source": "payload-root-Quote.Vehicle",
            "hierarchy": 1,
        },
        "id": {
            "partitionBy": ["quote_id", "vehicle_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote-Vehicle.Retainer",
            "hierarchy": 2,
        },
    },
    "out_radar_motor_quote": {
        "id": {
            "partitionBy": ["id"],
            "orderBy": ["id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
    },
    "out_radar_rescue_driver_on_vehicle": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "vehicle_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["vehicle_id"],
            "source": "payload-root-Quote.Vehicle",
            "hierarchy": 1,
        },
        "id": {
            "partitionBy": ["quote_id", "vehicle_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote-Vehicle.DriverOnVehicle",
            "hierarchy": 2,
        },
    },
    "out_radar_motor_vehicle": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote.Vehicle",
            "hierarchy": 1,
        },
    },
    "out_radar_quote_uwreferral": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote.UWReferral",
            "hierarchy": 1,
        },
    },
    "out_radar_rescue_vehicle": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote.Vehicle",
            "hierarchy": 1,
        },
    },
    "out_radar_vehicle_promo_applied": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "vehicle_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["vehicle_id"],
            "source": "payload-root-Quote.Vehicle",
            "hierarchy": 1,
        },
        "id": {
            "partitionBy": ["quote_id", "vehicle_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote-Vehicle.PromoApplied",
            "hierarchy": 2,
        },
    },
    "out_radar_rescue_quote": {
        "id": {
            "partitionBy": ["id"],
            "orderBy": ["id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
    },
    "out_radar_vehicle_endorsement": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "vehicle_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["vehicle_id"],
            "source": "payload-root-Quote.Vehicle",
            "hierarchy": 1,
        },
        "id": {
            "partitionBy": ["quote_id", "vehicle_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote-Vehicle.Endorsement",
            "hierarchy": 2,
        },
    },
    "out_radar_motor_veh_licence": {
        "quote_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["quote_id"],
            "source": "payload.root.Quote",
            "hierarchy": 0,
        },
        "vehicle_id": {
            "partitionBy": ["quote_id"],
            "orderBy": ["vehicle_id"],
            "source": "payload-root-Quote.Vehicle",
            "hierarchy": 1,
        },
        "id": {
            "partitionBy": ["quote_id", "vehicle_id"],
            "orderBy": ["id"],
            "source": "payload-root-Quote-Vehicle.Licence",
            "hierarchy": 2,
        },
    }
}
