import warnings

from tqdm import TqdmExperimentalWarning

# TqdmExperimentalWarning is raised when using tqdm with rich
warnings.filterwarnings("ignore", category=TqdmExperimentalWarning)
