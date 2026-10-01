from typing import Any

from ..cs3mixin import CS3Mixin
from ..cs3api.groups import CS3Groups
from ..cs3api.sharing import CS3Sharing
from ..cs3api.spaces import CS3Spaces
from ..cs3api.users import CS3Users


class CS3SharingClient(CS3Mixin):
    """
    Standalone CS3 client used by the sharing server extension
    which does not depend on the CS3 contents manager.
    """

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self.sharing = CS3Sharing(self.session)
        self.users = CS3Users(self.session)
        self.groups = CS3Groups(self.session)
        self.spaces = CS3Spaces(self.session)
