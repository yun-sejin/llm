from llm.dags.validate import ValidateDocument, ValidateFile


class ValidateFactory:
    @staticmethod
    def create_validator(validator_type):
        """
        Factory function to create a validator object based on the specified type.

        Args:
            validator_type (str): The type of validator to create. 
                                  Supported types are "document" and "file".

        Returns:
            object: An instance of the appropriate validator class.

        Raises:
            ValueError: If the provided validator_type is not supported.
        """
        if validator_type == "document":
            return ValidateDocument()
        elif validator_type == "file":
            return ValidateFile()
        else:
            raise ValueError(f"Unknown validator type: {validator_type}")