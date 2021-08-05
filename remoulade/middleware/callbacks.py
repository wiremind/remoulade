# This file is a part of Remoulade.
#
# Copyright (C) 2017,2018 CLEARTYPE SRL <bogdan@cleartype.io>
#
# Remoulade is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.
#
# Remoulade is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
# License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from .middleware import Middleware


class Callbacks(Middleware):
    """Middleware that lets you chain success and failure callbacks
    onto Actors.

    Parameters:
      on_failure(str): The name of an actor to send a message to on
        failure.
      on_success(str): The name of an actor to send a message to on
        success.
    """

    @property
    def actor_options(self):
        return {
            "on_failure",
            "on_success",
        }

    def after_process_message(self, broker, message, *, result=None, exception=None):
        if exception is None:
            actor_name = self.get_option("on_success", broker=broker, message=message)
            if actor_name:
                actor = broker.get_actor(actor_name)
                actor.send(message.asdict(), result)

        else:
            actor_name = self.get_option("on_failure", broker=broker, message=message)
            if actor_name:
                actor = broker.get_actor(actor_name)
                actor.send(message.asdict(), {"type": type(exception).__name__, "message": str(exception)})

    def update_options_before_create_message(self, options, broker, actor_name):
        from ..actor import Actor

        for option_name in ["on_failure", "on_success"]:
            callback = options.get(option_name)
            if isinstance(callback, Actor):
                options[option_name] = callback.actor_name

            elif callback is not None and not isinstance(callback, str):
                raise TypeError(f"{option_name} must be an Actor or a string, got {type(callback)} instead")

        return options
