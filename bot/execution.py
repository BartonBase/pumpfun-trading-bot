import os


class LiveExecutionDisabled(Exception):
    pass


def place_order(cfg, mint, side, size_sol):
    ex = cfg["execution"]
    if not ex.get("enabled"):
        raise LiveExecutionDisabled("execution.enabled is false: paper trading only")
    if not os.environ.get(ex["key_env_var"]):
        raise LiveExecutionDisabled(f"env var {ex['key_env_var']} not set: refusing")
    if size_sol > ex["max_size_sol"]:
        raise LiveExecutionDisabled(f"size {size_sol} SOL above execution.max_size_sol {ex['max_size_sol']}")
    raise LiveExecutionDisabled("this build has no signer: live orders are not implemented")
