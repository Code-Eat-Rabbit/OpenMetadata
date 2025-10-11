#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
This script generates the Python models from the JSON Schemas definition. Additionally, it replaces the `SecretStr`
pydantic class used for the password fields with the `CustomSecretStr` pydantic class which retrieves the secrets
from a configured secrets' manager.
"""
import datamodel_code_generator.model.pydantic
from datamodel_code_generator.imports import Import
import glob
import os
import re


datamodel_code_generator.model.pydantic.types.IMPORT_SECRET_STR = Import.from_full_path(
    "metadata.ingestion.models.custom_pydantic.CustomSecretStr"
)

from datamodel_code_generator.__main__ import main

current_directory = os.getcwd()
ingestion_path = "./" if current_directory.endswith("/ingestion") else "ingestion/"
directory_root = "../" if current_directory.endswith("/ingestion") else "./"

UTF_8 = "UTF-8"
UNICODE_REGEX_REPLACEMENT_FILE_PATHS = [
    f"{ingestion_path}src/metadata/generated/schema/entity/classification/tag.py",
    f"{ingestion_path}src/metadata/generated/schema/entity/events/webhook.py",
    f"{ingestion_path}src/metadata/generated/schema/entity/teams/user.py",
    f"{ingestion_path}src/metadata/generated/schema/entity/type.py",
    f"{ingestion_path}src/metadata/generated/schema/type/basic.py",
]

args = f"--input {directory_root}openmetadata-spec/src/main/resources/json/schema --output-model-type pydantic_v2.BaseModel --use-annotated --base-class metadata.ingestion.models.custom_pydantic.BaseModel --input-file-type jsonschema --output {ingestion_path}src/metadata/generated/schema --set-default-enum-member".split(" ")

main(args)

for file_path in UNICODE_REGEX_REPLACEMENT_FILE_PATHS:
    with open(file_path, "r", encoding=UTF_8) as file_:
        content = file_.read()
        # Python now requires to move the global flags at the very start of the expression
        content = content.replace("(?U)", "(?u)")
    with open(file_path, "w", encoding=UTF_8) as file_:
        file_.write(content)

# Until https://github.com/koxudaxi/datamodel-code-generator/issues/1895
# TODO: This has been merged but `Union` is still not there. We'll need to validate
MISSING_IMPORTS = [f"{ingestion_path}src/metadata/generated/schema/entity/applications/app.py",]
WRITE_AFTER = "from __future__ import annotations"

for file_path in MISSING_IMPORTS:
    with open(file_path, "r", encoding=UTF_8) as file_:
        lines = file_.readlines()
    with open(file_path, "w", encoding=UTF_8) as file_:
        for line in lines:
            file_.write(line)
            if line.strip() == WRITE_AFTER:
                file_.write("from typing import Union  # custom generate import\n\n")


# unsupported rust regex pattern for pydantic v2
# https://docs.pydantic.dev/2.7/api/config/#pydantic.config.ConfigDict.regex_engine
# We'll remove validation from the client and let it fail on the server, rather than on the model generation
UNSUPPORTED_REGEX_PATTERN_FILE_PATHS = [
    f"{ingestion_path}src/metadata/generated/schema/type/basic.py",
    f"{ingestion_path}src/metadata/generated/schema/entity/data/searchIndex.py",
    f"{ingestion_path}src/metadata/generated/schema/entity/data/table.py",
]

for file_path in UNSUPPORTED_REGEX_PATTERN_FILE_PATHS:
    with open(file_path, "r", encoding=UTF_8) as file_:
        content = file_.read()
        content = content.replace("pattern='^((?!::).)*$',", "")
    with open(file_path, "w", encoding=UTF_8) as file_:
        file_.write(content)

# Until https://github.com/koxudaxi/datamodel-code-generator/issues/1996
# Supporting timezone aware datetime is too complex for the profiler
DATETIME_AWARE_FILE_PATHS = [
    f"{ingestion_path}src/metadata/generated/schema/type/basic.py",
]

for file_path in DATETIME_AWARE_FILE_PATHS:
    with open(file_path, "r", encoding=UTF_8) as file_:
        content = file_.read()
        content = content.replace(
            "from pydantic import AnyUrl, AwareDatetime, ConfigDict, EmailStr, Field, RootModel",
            "from pydantic import AnyUrl, ConfigDict, EmailStr, Field, RootModel"
        )
        content = content.replace("from datetime import date, time", "from datetime import date, time, datetime")
        content = content.replace("AwareDatetime", "datetime")
    with open(file_path, "w", encoding=UTF_8) as file_:
        file_.write(content)

# Fix RootModel with extra config
# RootModel does not support setting model_config['extra'] in Pydantic 2.x
# This fixes the error: PydanticUserError: `RootModel` does not support setting `model_config['extra']`
# We need to remove the extra='forbid' config from any RootModel classes

GENERATED_SCHEMA_PATH = f"{ingestion_path}src/metadata/generated/schema"
rootmodel_files = glob.glob(f"{GENERATED_SCHEMA_PATH}/**/*.py", recursive=True)

for file_path in rootmodel_files:
    try:
        with open(file_path, "r", encoding=UTF_8) as file_:
            content = file_.read()
        
        # Check if file contains RootModel classes
        if "class " in content and "RootModel[" in content:
            lines = content.split('\n')
            new_lines = []
            in_rootmodel_class = False
            skip_next_model_config = False
            
            for i, line in enumerate(lines):
                # Check if this line defines a RootModel class
                if "class " in line and "RootModel[" in line:
                    in_rootmodel_class = True
                    skip_next_model_config = False
                    new_lines.append(line)
                    continue
                
                # Check if we're in a RootModel class and encounter model_config with extra
                if in_rootmodel_class and "model_config = " in line and "extra=" in line:
                    # Check if model_config only contains 'extra', if so skip the entire line
                    if "ConfigDict(extra=" in line and line.strip().endswith(")"):
                        # Skip this line entirely if it only contains extra config
                        # Check next few lines to see if this is the only config
                        peek_ahead = ""
                        for j in range(i+1, min(i+3, len(lines))):
                            peek_ahead += lines[j]
                        
                        # If the model_config only has extra, skip it
                        if "ConfigDict(extra='forbid')" in line or "ConfigDict(extra=\"forbid\")" in line:
                            skip_next_model_config = True
                            continue
                    
                    # Otherwise remove just the extra parameter
                    # Handle both single-line and multi-line ConfigDict
                    modified_line = re.sub(r",?\s*extra=['\"]forbid['\"],?\s*", "", line)
                    # Clean up resulting double commas or trailing commas before closing paren
                    modified_line = re.sub(r',\s*,', ',', modified_line)
                    modified_line = re.sub(r',\s*\)', ')', modified_line)
                    # If ConfigDict is now empty, skip the line
                    if "ConfigDict()" in modified_line:
                        continue
                    new_lines.append(modified_line)
                    continue
                
                # Reset flag when we hit the next class or significant dedent
                if in_rootmodel_class and line and not line[0].isspace() and "class " in line:
                    in_rootmodel_class = False
                    
                new_lines.append(line)
            
            modified_content = '\n'.join(new_lines)
            
            # Only write if content changed
            if modified_content != content:
                with open(file_path, "w", encoding=UTF_8) as file_:
                    file_.write(modified_content)
                    
    except Exception as e:
        # Don't fail the entire generation if one file has issues
        print(f"Warning: Could not process {file_path}: {e}")
