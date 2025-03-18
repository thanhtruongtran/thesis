from pydantic import BaseModel


class News(BaseModel):
    """
    Represents the data structure of a News.
    """

    title: str
    link: str
    publishedTime: str
    category: str
    description: str
    content: str
