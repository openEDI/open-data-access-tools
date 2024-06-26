# Super-Resolution for Renewable Energy Resource Data with Climate Change Impacts (Sup3rCC)


## Overview

The Super-Resolution for Renewable Energy Resource Data with Climate Change Impacts (Sup3rCC) data is a collection of 4km hourly wind, solar, temperature, humidity, and pressure fields for the contiguous United States under climate change scenarios.

Sup3rCC is downscaled Global Climate Model (GCM) data. For example, the initial dataset "sup3rcc_conus_mriesm20_ssp585_r1i1p1f1" is downscaled from MRI ESM 2.0 for climate change scenario SSP5 8.5 and variant label r1i1p1f1. The downscaling process was performed using a generative machine learning approach called sup3r: Super-Resolution for Renewable Energy Resource Data ([Sup3r GitHub Repo](https://github.com/NREL/sup3r)). The data includes both historical and future weather years, although the historical years represent the historical average climate, not the actual historical weather that we experienced.

The Sup3rCC data is intended to help researchers study the impact of climate change on energy systems with high levels of wind and solar capacity. Please note that all climate change data is only a representation of the *possible* future climate and contains significant uncertainty. Analysis of multiple climate change scenarios and multiple climate models can help quantify this uncertainty.

For more info, view the [OEDI Sup3rcc catalogue entry](https://data.openei.org/submissions/5839).

## Storage Resources

The Sup3rcc Dataset is made available in h5 format in the following container:

`https://nrel.blob.core.windows.net/oedi`

### Data

The data are located in the `sup3rcc/` directory. The initial datset is the subdirectory `conus_mriesm20_ssp585_r1i1p1f1/`.

Each h5 file's name encodes info about the variables it contains and the year.

e.g. `sup3rcc_conus_mriesm20_ssp585_r1i1p1f1_pressure_2015.h5`

### Data Format

The Sup3rcc dataset is provided in h5 format. A kerchunk reference file is also included to facilitate faster access.
 
#### `Dimensions:`
field | data_type
-- | --
`time_index` | int
`latitude` | float
`longitude` | float

#### `Location Metadata:`

field | data_type
-- | --
`country` | string
`state` | string
`county` | string
`timezone` | string
`eez` | string
`elevation` | string
`offshore` | string

#### `Variables:`

field | data_type
-- | --
`dhi` | float
`dni` | float
`ghi` | float
`pressure_0m` | float
`relativehumidity_2m` | float
`temperature_2m` | float
`winddirection_100m` | float
`winddirection_10m` | float
`winddirection_200m` | float
`windspeed_100m` | float
`windspeed_10m` | float
`windspeed_200m` | float
`offshore` | float

## Sample code

A complete Python example of accessing and visualizing some of these data is available in the accompanying [sample notebook](https://nbviewer.jupyter.org/github/microsoft/AIforEarthDataSets/blob/main/data/sup3rcc.ipynb).

## Mounting the container

We also provide a read-only SAS (shared access signature) token to allow access via, e.g., [BlobFuse](https://github.com/Azure/azure-storage-fuse), which allows you to mount blob containers as drives:

`https://nrel.blob.core.windows.net/oedi?sv=2019-12-12&si=oedi-ro&sr=c&sig=uslpLxKf3%2Foeu79ufIHbJkpI%2FTWDH3lblJMa5KQRPmM%3D`

Mounting instructions for Linux are [here](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-how-to-mount-container-linux).

## Disclaimer and Attribution

Copyright (c) 2020, Alliance for Sustainable Energy LLC, All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


## Contact

For questions about this dataset, contact [`aiforearthdatasets@microsoft.com`](mailto:aiforearthdatasets@microsoft.com?subject=oedi%20question).


## Notices

Microsoft provides this dataset on an "as is" basis.  Microsoft makes no warranties (express or implied), guarantees, or conditions with respect to your use of the dataset.  To the extent permitted under your local law, Microsoft disclaims all liability for any damages or losses * including direct, consequential, special, indirect, incidental, or punitive * resulting from your use of this dataset.  This dataset is provided under the original terms that Microsoft received source data.